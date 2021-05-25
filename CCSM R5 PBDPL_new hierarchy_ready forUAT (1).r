# Databricks notebook source
StartTime <- Sys.time()

# COMMAND ----------

# load the two packages that we need for modeling
library(MLForecastPackage)
library(SparkR)
library(tidyr)
library(lubridate)

# COMMAND ----------

dbutils.widgets.combobox("frequency", "12", list("4", "12", "52"))
dbutils.widgets.text("maxHorizons", "12")
dbutils.widgets.text("targetColumnName", "Sell-Thru")
dbutils.widgets.combobox("SecretScope", "FBISAMLDevKeyVault", list("FBISAMLDevKeyVault", "FBISAMLUATKeyVault"))
dbutils.widgets.combobox("SecretKey", "DevMLConnString", list("DevMLConnString", "UATAMLConn"))
dbutils.widgets.dropdown("doCoherentForecast","FALSE",list("FALSE","TRUE"))

# COMMAND ----------

failureTable <- "CDS_FailureLogging_R5_launch"
outputTable <- "CCSMRevenueForecasts_ADB_R5_MLForecast_Newhierarchy"
coherentoutputTable <- ' CCSMRevenueForecasts_ADB_R5_Coherent_Newhierarchy'

# COMMAND ----------

connString <- dbutils.secrets.get(
  scope=dbutils.widgets.get("SecretScope"), 
  key=dbutils.widgets.get("SecretKey")
)

secrets <- lapply(unlist(strsplit(connString, split=";", fixed=FALSE)), function(x) {
    unlist(strsplit(x, split="=", fixed=FALSE))
  }
)

secretsList <- setNames(sapply(secrets, "[[", 2), sapply(secrets, "[[", 1))

jdbcURL = paste0("jdbc:sqlserver://", secretsList['Data Source'], ";database=", secretsList['Initial Catalog'])
jdbcUser = secretsList['User ID']
jdbcPassword = secretsList['Password']

# COMMAND ----------

print(jdbcURL)

# COMMAND ----------

inputSQL <- "( select DateIndex, Hierarchy, Feature, [Value] from [vw_StandardInputView_Monthly] where Domain='CCSM' 
) as e"

# COMMAND ----------

data <- as.data.frame(read.jdbc(url=jdbcURL, tableName=inputSQL, user=jdbcUser, password=jdbcPassword))

# COMMAND ----------

display(data)

# COMMAND ----------

 names(data)[names(data) == 'DateIndex'] <- 'DateColumn'
names(data)[names(data) == 'Hierarchy'] <- 'Hierarchy_ID'
names(data)[names(data) == 'Feature'] <- 'Feature_ID'

# COMMAND ----------

# create a settings list from the widget inputs
settings <- list(
  frequency=as.integer(dbutils.widgets.get("frequency")),
  targetColumnName=dbutils.widgets.get("targetColumnName"),
  maxHorizons=as.integer(dbutils.widgets.get("maxHorizons"))
)
settings

# COMMAND ----------

coviddata<- dplyr::filter(data,data$Feature_ID=='Covid' & data$DateColumn=='2020-03-01')
data<- dplyr::filter(data,!(data$Feature_ID=='Covid' & data$DateColumn=='2020-03-01'))
coviddata$Value<- 0
data<- rbind(data,coviddata)
coviddata<- dplyr::filter(data,data$Feature_ID=='Covid' & data$DateColumn=='2020-02-01')
data<- dplyr::filter(data,!(data$Feature_ID=='Covid' & data$DateColumn=='2020-02-01'))
coviddata$Value<- 1
data<- rbind(data,coviddata)

# COMMAND ----------

# create a directory for saving off model objects
initTime <- format(Sys.time(), '%Y-%m-%d_%H%M%S')
modelOutputPath <- file.path('/dbfs/FileStore/ModelArtifacts/CDS', initTime)
if (!dir.exists(modelOutputPath)) {
  dir.create(modelOutputPath, recursive=TRUE)
  print("created directory:")
  print(modelOutputPath)
} else {
  print("directory exists:")
  print(modelOutputPath)
}

# COMMAND ----------

tsToForecast<-list()
tsToForecast_Launch<- list()
for(i in unique(data[,c("Hierarchy_ID")])){
  df <- data[which(data[,c("Hierarchy_ID")]==i),]
  launch_temp<- subset(df,df$Feature_ID %in% df$Feature_ID[grep("Launch",df$Feature_ID)])
  tsToForecast_Launch <- append(tsToForecast_Launch,TransformLongData(launch_temp, freq=settings$frequency))
  
   df<- subset(df,!(df$Feature_ID %in% df$Feature_ID[grep("Launch",df$Feature_ID)]))
  mindate<- min(dplyr::filter(df,df[["Feature_ID"]]=="Sell-Thru")$DateColumn)
  temp<- dplyr::filter(df,df[["DateColumn"]]>=mindate)
  maxdate<- max(dplyr::filter(temp,temp[["Feature_ID"]]=="Sell-Thru")$DateColumn)
  temp<- dplyr::filter(temp,temp[["DateColumn"]]<=maxdate)
  tsToForecast=append(tsToForecast,TransformLongData(temp, freq=settings$frequency))

}


# COMMAND ----------

# transform the data into the long format data
# tsToForecast <- TransformLongData(data, freq=settings$frequency)
namesOfModels <- names(tsToForecast)
length(tsToForecast)

# COMMAND ----------

# filter out those models which have a length of one
mask <- which(sapply(tsToForecast, function(x) length(x)) > 1)

namesOfModels <- namesOfModels[mask]
tsToForecast <- tsToForecast[mask]

print(length(tsToForecast))

# COMMAND ----------

# filter out models whos total sums to 0
mask <- which(sapply(tsToForecast, function(x) sum(x, na.rm=TRUE)) != 0)

namesOfModels <- namesOfModels[mask]
tsToForecast <- tsToForecast[mask]

print(length(tsToForecast))

# COMMAND ----------

#construct dataset with launch data as feature data
calLaunchdata<- function(model,input_launch){
  model$launchLags<- -1:-6

    model$featureDatawithlaunch <- baselineforecast::ConstructDataset(
    series = model$targetData,
    extraData = input_launch,
    maxHorizon = model$maxHorizons,
    seriesName = model$modelName,
    burnIn = model$burnIn,
    dateCutoffs = c(model$valStart, model$testStart),
    lagsToConstructExtras = model$launchLags,
    doArima = model$tsModels$arima,
    doEts = model$tsModels$ets,
    doStl = model$tsModels$stl
    )


  return(model)
}

# COMMAND ----------

# define the model pipeline
modelPipeline <- function(tsObj, modelName, settings,ts_launch_extra=NULL) {
  
  # Initialize the model object from the timeseries objects
  model <- SingleForecastModel(
    inputData=tsObj, 
    modelName=modelName, 
    targetColumnName=settings$targetColumnName, 
    maxHorizons=settings$maxHorizons,
    featureLags=c(1:12),
    parallelFitModel=FALSE
  )
  
  # Fill missing values with zeros
  model <- imputeMissingValues(model, how='fill', fill_val = 0)
  
  # by default set the attribute 'failed' to FALSE
  model$failed <- FALSE
  model$initializedTime <- Sys.time()
  
  rdsFileName = paste0(make.names(model$modelName), '.rds')
  
  
  
  # If the model is for average forecasting, only compute the rolling average forecast
  # then return the model object.
  if (model$forecastType == 'avg') {
    ## Do Average Forecasting
    model <- tryCatch(
    ComputeRollingAverage(model),
      error = function(c) {
        model$failed <- TRUE
        model$failedStep <- "RollingAverage"
        model$errorMessage <- c$message
        return(model)
      }
    )
  }
  

  
   if ((!model$failed) & (model$forecastType %in% c('ml', 'tsall', 'tsnostl'))) {
    
    ## Do ConstructDatset
    model <- tryCatch({
  if(!is.null(ts_launch_extra)){
   
      model<- calLaunchdata(model,ts_launch_extra)
       model<- ConstructDataset(model)
       if(grep("launch",names(model))) {
    model$constructDatasetResults<- merge(x= model$constructDatasetResults,y= model$featureDatawithlaunch[c("stringIndex","horizon",colnames(model$featureDatawithlaunch)[c(grep("launch",colnames(model$featureDatawithlaunch)))])],
                                          by.x = c("stringIndex","horizon"), by.y = c("stringIndex","horizon"), all.x = TRUE)
        }
    } else {
  
          model<-ConstructDataset(model)
        }
      model
    },
      error = function(c) {
        model$failed <- TRUE
        model$failedStep <- "ConstructDataset"
        model$errorMessage <- c$message
        return(model)
      }
    )
      
  }

# model
#   If the model is ML and it didn't fail in the previous step, do FitModel
#   on the model object
  if ((!model$failed) & (model$forecastType %in% c('ml'))) {
    ## Do FitModel
    model <- tryCatch(
     FitModel(model), 
      error = function(c) {
        model$failed <- TRUE
        model$failedStep <- "FitModel"
        model$errorMessage <- c$message
        return(model)
      }
    )
  }

  # Run the ConstructBestForecast step if model forecast type is not rolling average
  if ((!model$failed) & (model$forecastType != 'avg')) {
    ## Do ConstructBestForecast
    model <- tryCatch(
      ConstructBestForecast(model),
      error = function(c) {
        model$failed <- TRUE
        model$failedStep <- "ConstructBestForecast"
        model$errorMessage <- c$message
        return(model)
      }
    )
  }
  
  # If the model is ML and it didn't fail in the previous step, do Ensemble
  # on the model object
  if ((model$forecastType  %in% c('ml')) & (!model$failed)) {
    ## Do FitEnsemble
    model <- tryCatch(
      FitEnsemble(model),
      error = function(c) {
        model$failed <- TRUE
        model$failedStep <- "FitEnsemble"
        model$errorMessage <- c$message
        return(model)
      })
  }

  # Regardless of the model type, run the CombineForecastResults step
  if (!model$failed) {
    ## Do CombineForecastResults
    model <- tryCatch(
      CombineForecastResults(model),
      error = function(c) {
        model$failed <- TRUE
        model$failedStep <- "CombineForecastResults"
        model$errorMessage <- c$message
        return(model)
      })
  }
  
  # Regardless of the model type, run the ComputePredictionIntervals step
  if (!model$failed) {
    ## Do ComputePredictionIntervals
    model <- tryCatch(
      ComputePredictionIntervals(model),
      error = function(c) {
        model$failed <- TRUE
        model$failedStep <- "ComputePredictionIntervals"
        model$errorMessage <- c$message
        return(model)
      })
  }

  saveRDS(model, file.path(modelOutputPath, rdsFileName))

  if (model$failed) {
    return(list(failed=TRUE, time=model$initializedTime, hierarchyID=model$modelName, forecastType=model$forecastType, step=model$failedStep, message=model$errorMessage))
  } else {
    forecasts <- model$combinedForecastResultsWithPints
    
    forecasts$hierarchyID <- model$modelName
    forecasts$initTime <- model$initializedTime

    return(list(failed=FALSE, hierarchyID=model$modelName, forecastType=model$forecastType, forecasts=forecasts))
  }
}

# COMMAND ----------

# run the model pipeline
outputs <- spark.lapply(seq_along(tsToForecast), function(i) {
  library(MLForecastPackage)
  library(zoo)
  library(lubridate)
  library(dplyr)
  tsToForecast[[i]][is.na(tsToForecast[[i]])] <- 0
  output <- modelPipeline(tsToForecast[[i]], namesOfModels[[i]], settings,tsToForecast_Launch[[names(tsToForecast)[[i]]]])
  return(output)
  
})

# COMMAND ----------

print(jdbcURL)
print(outputTable)

# COMMAND ----------

outputs <- setNames(outputs, sapply(outputs, function(x) x$hierarchyID))

compact <- function(x) Filter(Negate(is.null), x)

# extract the failed models and write to failure table
failed <- compact(lapply(outputs, function(x) {
  if (x$failed) {
    return(data.frame(x))
  }
}))

if (length(failed) > 0) {
  print(paste("failed:", length(failed)))
  failed_df <- data.table::rbindlist(failed, use.names=TRUE)
#   write.jdbc(createDataFrame(failed_df, numPartitions=100), jdbcURL, failureTable, user=jdbcUser, password=jdbcPassword, mode='append')  
}


# extract the succeeded models and write to output table
succeeded <- compact(lapply(outputs, function(x) {
  if (!x$failed) {
    return(x$forecasts)
  }
}))

if (length(succeeded) > 0) {
  print(paste("succeeded:", length(succeeded)))
  succeeded_df <- data.table::rbindlist(succeeded, use.names=TRUE)
  
  # Convert horizon from factor to integer
  succeeded_df[["horizon"]] <- as.integer(succeeded_df[["horizon"]])
  
  hierarchy_divisions <- c("Product", "PBD","CreditedWorldwideSubRegion")
  
  succeeded_df <- succeeded_df %>%
    separate(col = hierarchyID, into = hierarchy_divisions, sep = "\\|\\|", fill = "right") %>% 
    dplyr::mutate(
      StartTime = StartTime,
      EndTime = Sys.time()
    )
   succeeded_df$target[is.infinite(succeeded_df$target)]<- 0
   write.jdbc(createDataFrame(succeeded_df, numPartitions=100), jdbcURL, outputTable, user=jdbcUser, password=jdbcPassword, mode='append')
#   write.jdbc(createDataFrame(succeeded_df, numPartitions=100), jdbcURL, "CDSRevenueForecasts_ADB_R5", user=jdbcUser, password=jdbcPassword, mode='append')
}

# COMMAND ----------

install.packages('hts')
devtools::install_github("robjhyndman/thief")
library(thief)

# COMMAND ----------

library(SparkR)
library(dplyr)
library(tidyr)
library(stringr)
library(purrr)
# library(ggplot2)
library(zoo)
library(lubridate)
# library(forecast)
library(hts)
library(doParallel)
# library(caret)
library(sqldf)

# COMMAND ----------

connString <- dbutils.secrets.get(
  scope=dbutils.widgets.get("SecretScope"), 
  key=dbutils.widgets.get("SecretKey")
)

secrets <- lapply(unlist(strsplit(connString, split=";", fixed=FALSE)), function(x) {
    unlist(strsplit(x, split="=", fixed=FALSE))
  }
)

secretsList <- setNames(sapply(secrets, "[[", 2), sapply(secrets, "[[", 1))

jdbcURL = paste0("jdbc:sqlserver://", secretsList['Data Source'], ";database=", secretsList['Initial Catalog'])
jdbcUser = secretsList['User ID']
jdbcPassword = secretsList['Password']

# COMMAND ----------

print(coherentoutputTable)

# COMMAND ----------

inputSQL <- "(
select Product,PBD, CreditedWorldwideSubRegion, horizon,dateIndex, target, forecast,method,isBest from CCSMRevenueForecasts_ADB_R5_MLForecast_Newhierarchy where quantile = 0.8

) as inputData"
dataset <- as.data.frame(read.jdbc(url=jdbcURL, tableName=inputSQL, user=jdbcUser, password=jdbcPassword))

# # Cast dateIndex to date'
dataset$dateIndex <- zoo::as.Date(dataset$dateIndex)

# Cast some columns to numeric
dataset$target <- as.numeric(dataset$target)
dataset$forecast <- as.numeric(dataset$forecast)
mindate<- min((dplyr::filter(dataset,dataset$method=="ensembleCombo"))[["dateIndex"]])
str(dataset)

# COMMAND ----------

#select ensembleCombo method, if not present then select through isBest.
Frames <- split(x = dataset, f = dataset[["PBD"]])
dataset<- lapply(Frames,function(x){
  x[["PBD"]]<- NULL
  data_split<- split(x = x, f = x[["CreditedWorldwideSubRegion"]])
  data<- lapply(data_split,function(y){
    y[["CreditedWorldwideSubRegion"]]<- NULL
    if('ensembleCombo' %in% unique(y[["method"]])){
      temp<- subset(y,y[["method"]]=='ensembleCombo')
    } else {
      temp<- subset(y,y[["isBest"]]==1)
    }
    temp
  })
   data<- plyr::ldply(data, data.frame, .id = "CreditedWorldwideSubRegion")
  data
})
dataset<-  plyr::ldply(dataset, data.frame, .id = "PBD")
dataset[["method"]]<- NULL
dataset[["isBest"]]<- NULL
datass<- dataset

# COMMAND ----------

doCoherent<- function(datass){
  all_fcsts_long_dff = data.frame()
  for(i in unique(datass[,c("horizon")])){
    df <- datass[which(datass[,c("horizon")]==i),]
    data<-sqldf("select Product,PBD, CreditedWorldwideSubRegion,dateIndex, target, forecast from df")
    data$dateIndex <- zoo::as.Date(data$dateIndex)
    data$target <- as.numeric(data$target)
    data$forecast <- as.numeric(data$forecast)
    startDate = min(data$dateIndex)
    endDate = max(data$dateIndex)
    input_actuals <- data %>%
      dplyr::select(dateIndex, Product,PBD, CreditedWorldwideSubRegion, target)
    
    input_forecasts <- data %>%
      dplyr::select(dateIndex, Product,PBD, CreditedWorldwideSubRegion, forecast)
    
    actuals_wide <- input_actuals %>%
      tidyr::unite(hierarchy_id, Product,PBD, CreditedWorldwideSubRegion) %>%
      tidyr::spread(hierarchy_id, target) %>%
      dplyr::select(-dateIndex)
    
    forecasts_wide <- input_forecasts %>%
      tidyr::unite(hierarchy_id, Product,PBD, CreditedWorldwideSubRegion) %>%
      tidyr::spread(hierarchy_id, forecast) %>%
      dplyr::select(-dateIndex)
    hierarchy_ids <- data.frame(hierarchy_id = names(forecasts_wide))
    hierarchy_ids_sep <- separate(hierarchy_ids, hierarchy_id, c('Product', 'PBD','CreditedWorldwideSubRegion'), '_') #change
# PBD - IP data
    
    Total_idx <- which(hierarchy_ids_sep$Product=="Total" & hierarchy_ids_sep$PBD=="Total" &hierarchy_ids_sep$CreditedWorldwideSubRegion=="Total") #str - 'total' (change)
    Product_idx <- which(hierarchy_ids_sep$Product!="Total" & hierarchy_ids_sep$PBD=="Total" &hierarchy_ids_sep$CreditedWorldwideSubRegion=="Total") # 
  PBD_idx <- which(hierarchy_ids_sep$Product!="Total" & hierarchy_ids_sep$PBD !="Total"&hierarchy_ids_sep$CreditedWorldwideSubRegion=="Total")
    CreditedWorldwideSubRegion_idx <-which(hierarchy_ids_sep$Product=="Total" &  hierarchy_ids_sep$PBD=="Total" & hierarchy_ids_sep$CreditedWorldwideSubRegion!="Total")
    Bottom_idx <- which(hierarchy_ids_sep$Product!="Total"&  hierarchy_ids_sep$PBD!="Total" & hierarchy_ids_sep$CreditedWorldwideSubRegion!="Total")
  # manually identify the corphq and put in right order wrt gts  
    cp_groups <- t(hierarchy_ids_sep[Bottom_idx,]) # 
    
    actuals_wide_ordered <- cbind(
      actuals_wide[, Total_idx, drop=FALSE],
      actuals_wide[, Product_idx, drop=FALSE], # Add PBd line
      actuals_wide[, PBD_idx, drop=FALSE],
      actuals_wide[, CreditedWorldwideSubRegion_idx, drop=FALSE],
      actuals_wide[, Bottom_idx, drop=FALSE]
    )
    
    actuals_mts <- ts(actuals_wide_ordered, start = c(lubridate::year(startDate), lubridate::month(startDate)), frequency = 12)
  
    forecasts_wide_ordered <- cbind(
      forecasts_wide[, Total_idx, drop=FALSE],
      forecasts_wide[, Product_idx, drop=FALSE], # add pbd line
      forecasts_wide[, PBD_idx, drop=FALSE],
      forecasts_wide[, CreditedWorldwideSubRegion_idx, drop=FALSE],
      forecasts_wide[, Bottom_idx, drop=FALSE]
    )
    
    forecasts_mts <- ts(forecasts_wide_ordered, start = c(lubridate::year(startDate), lubridate::month(startDate)), frequency = 12)
    
    forecasts_wide_bottom <- forecasts_wide[, Bottom_idx]
    forecasts_bottom_mts <- ts(forecasts_wide_bottom, start = c(lubridate::year(startDate), lubridate::month(startDate)), frequency = 12)
    
    forecasts_gts <- gts(forecasts_bottom_mts, groups = cp_groups) # gts columns should be in the same order as mts - idx drop and add (if in case of discrepency)
    tmp.resid <- actuals_mts - forecasts_mts
    wvec <- 1/colMeans(tmp.resid^2, na.rm = TRUE)
    
    coherentf <- combinef(na.omit(forecasts_mts), groups = cp_groups,weights = wvec, nonnegative=FALSE) #wvec missing
    
    all_fcsts <- aggts(coherentf, levels=4)
    
    all_fcsts_long_df <- all_fcsts %>%
      as.data.frame() %>%
      dplyr::mutate(numericIndex=as.numeric(time(all_fcsts))) %>%
      tidyr::gather(key="hierarchy_id", value="Forecast", -numericIndex) %>%
      dplyr::mutate(
        hierarchy_id = case_when(
          hierarchy_id == "Total" ~ "Total_Total",
          substr(hierarchy_id, 1, 2) == "G1" ~ paste0(substr(hierarchy_id, 4, length(hierarchy_id)), "_All"),
          substr(hierarchy_id, 1, 2) == "G2" ~ paste0("All_", substr(hierarchy_id, 4, length(hierarchy_id))),
          TRUE ~ hierarchy_id
        )
      ) %>%
      tidyr::separate(hierarchy_id, c("Product","PBD", "CreditedWorldwideSubRegion"), sep="_")
    actual_data <- input_actuals %>%
      mutate(
        numericIndex=as.numeric(zoo::as.yearmon(dateIndex)),
        Actual=target
      )
    
    output <- all_fcsts_long_df %>%
      merge(
        actual_data,
        by.x=c("Product","PBD", "CreditedWorldwideSubRegion", "numericIndex"),
        by.y=c("Product","PBD", "CreditedWorldwideSubRegion", "numericIndex"),
        all.x = TRUE,
        all.y = FALSE
      ) %>%
      mutate(
        #dateIndex=as.Date(zoo::as.yearqtr(numericIndex - 0.5)),
        FiscalQuarter=format(zoo::as.yearqtr(dateIndex %m+% months(6)), "FY%y-Q%q")
      ) %>%
      select(
        Product,
        PBD,
        CreditedWorldwideSubRegion,
        numericIndex,
        FiscalQuarter,
        dateIndex,
        Actual,
        Forecast
      )
    output$horizon<-i
    all_fcsts_long_dff<-rbind(all_fcsts_long_dff,output)
    
    
  }
  return(all_fcsts_long_dff)
}


# COMMAND ----------

# MAGIC %md #Quantile and useForEval

# COMMAND ----------

#Post coherent function
postCoherent<- function(all_fcsts_long_dff, firstFutureForecastMonth){
  fcastnames <- c("numericIndex", "horizon","Forecast")
  intervals <- c(0.8, 0.95)
  all_fcsts_long_dffff = data.frame()
  for(i in unique(all_fcsts_long_dff[,c("PBD")])){
    for(j in unique(all_fcsts_long_dff[,c("CreditedWorldwideSubRegion")])){
      for(k in unique(all_fcsts_long_dff[,c("horizon")])){
        df <- all_fcsts_long_dff[which(all_fcsts_long_dff[,c("PBD")]==i),]
        df <- df[which(df[,c("CreditedWorldwideSubRegion")]==j),]
        if (nrow(df)==0){
          next (j)
        } else {
        }
        df <- df[which(df[,c("horizon")]==k),]
        df$stringIndex<- ComputeStringIndex(df[["numericIndex"]],12)
        fc<- split(df, df[["PBD"]], drop = TRUE)
        
        pints <- lapply(setNames(names(fc), names(fc)),function(x) {
          actuals <- unique(fc[[x]][c("numericIndex", "Actual")])
          
          actuals <- actuals[order(actuals[["numericIndex"]]), ]
          actuals <- ts(actuals[["Actual"]], start = min(actuals[["numericIndex"]]), frequency = 12)
          
          result <- ConstructPredictionInterval(fc[[x]][names(fc[[x]]) %in% fcastnames],
                                                actuals, intervals)
          result[["PBD"]] <- x
          result
          
        })
        
        pints <- do.call(rbind, pints)
        output.pi <-reshape2::melt(pints,id = c("PBD", "stringIndex", "quantile", "horizon"),value.name = "width",variable.name = "method")
        output_value <- merge(df,
                              output.pi,
                              all.x = TRUE,
                              by = c("PBD", "stringIndex", "horizon")
        )
        missing <- do.call("rbind", replicate(length(intervals),output_value[is.na(output_value[["quantile"]]),],simplify = FALSE))
        missing[["quantile"]] <- rep(intervals, each = sum(is.na(output_value[["quantile"]])))
        output_value <- rbind(output_value[!is.na(output_value[["quantile"]]),], missing)
        output_value[["lower"]] <- output_value[["Forecast"]] - output_value[["width"]]
        output_value[["upper"]] <- output_value[["Forecast"]] + output_value[["width"]]
        all_fcsts_long_dffff<-rbind(all_fcsts_long_dffff,output_value)
      }
    }
  }
  
  
  finalForecasts<- all_fcsts_long_dffff
  finalForecasts$fiscalYearQtr <- format(as.yearqtr(finalForecasts$dateIndex %m+% months(6)), "FY%yQ%q")
  currentMonth <- floor_date(firstFutureForecastMonth, "month")
  currentQtr <- na.omit(finalForecasts$fiscalYearQtr[finalForecasts$dateIndex == currentMonth])[1]
  past <- subset(finalForecasts, fiscalYearQtr < currentQtr)
  future <- subset(finalForecasts, fiscalYearQtr >= currentQtr)
  past[["useForEval"]] <- 1 * (as.numeric(past[["horizon"]]) == 1)
  futureHorizonCheck <- future %>% dplyr::group_by(dateIndex) %>% dplyr::filter(as.numeric(horizon) == min(as.numeric(horizon)))
  futureHorizonCheck <- futureHorizonCheck %>% dplyr::select(dateIndex, horizon)
  df <- merge(future, futureHorizonCheck, by= "dateIndex")
  future <- df[!duplicated(df),]
  future[["useForEval"]] <- as.numeric(future$horizon.x == future$horizon.y)
  
  future[["horizon"]] <- future$horizon.x
  future$horizon.x = NULL
  future$horizon.y = NULL
  past$horizon.x = NULL
  past$horizon.y = NULL
  past$fiscalYearQtr<- NULL
  future$fiscalYearQtr<- NULL
  
  
  return(dplyr::bind_rows(past, future))
  
}


# COMMAND ----------

library(baselineforecast)

# COMMAND ----------

all_fcsts_long_dffff<- doCoherent(datass)
all_fcsts_long_dffff<-postCoherent(all_fcsts_long_dffff,as.Date(Sys.Date()))


# COMMAND ----------

all_fcsts_long_dffff$method<-NULL
all_fcsts_long_dffff$StartTime<-Sys.time()
all_fcsts_long_dffff$EndTime<-Sys.time()
display(all_fcsts_long_dffff)

# COMMAND ----------

str(all_fcsts_long_dffff)

# COMMAND ----------

write.jdbc(createDataFrame(all_fcsts_long_dffff, numPartitions=100), jdbcURL, coherentoutputTable, user=jdbcUser, password=jdbcPassword, mode='append')
