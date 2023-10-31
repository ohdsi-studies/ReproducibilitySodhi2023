# Additional Code for Sodhi --------------
runSodhiReplication <- function(connectionDetails,
                                cdmDatabaseSchema,
                                cohortDatabaseSchema,
                                cohortTable,
                                tempEmulationSchema,
                                outputFolder,
                                maxCores) {
  
  
  # create output folder
  sodhiOutputFolder <- file.path(outputFolder, "sodhiOutput")
  if (!file.exists(sodhiOutputFolder)) {
    dir.create(sodhiOutputFolder)
  }
  
  
  # table of covariate cohorts used in sodhi replication
  covariateCohorts <- tibble::tibble(
    cohortId = c(3157, 3158, 
                 3159, 3160),
    cohortName = c(
      "[JAMA Sodhi] Hyperlipidemia",
      "[JAMA Sodhi] Abdominal surgery",
      "[JAMA Sodhi] Smoking",
      "[JAMA Sodhi] alcohol"
    )
  )
  
  #demographic covariates gender and continuous age
  covSettings1 <- FeatureExtraction::createCovariateSettings(
    useDemographicsGender = TRUE,
    useDemographicsAge = TRUE
  )
  
  #cohort covariates with 1 yr lookback
  covSettings2 <- FeatureExtraction::createCohortBasedCovariateSettings(
    analysisId = 991,
    covariateCohorts = covariateCohorts %>% dplyr::filter(cohortId != 3158),
    startDay = -365,
    endDay = 0
  )
  
  #cohort covariates with 30 d lookback
  covSettings3 <- FeatureExtraction::createCohortBasedCovariateSettings(
    analysisId = 992,
    covariateCohorts = covariateCohorts %>% dplyr::filter(cohortId == 3158),
    startDay = -30,
    endDay = 0
  )
  
  #create full list of covariate settings
  covSettingsList <- list(covSettings1, covSettings2, covSettings3)
  
  
  # create cm data args
  getDbCmDataArgs1 <- CohortMethod::createGetDbCohortMethodDataArgs(
    washoutPeriod = 0,
    restrictToCommonPeriod = FALSE,
    firstExposureOnly = TRUE,
    removeDuplicateSubjects = "remove all",
    studyStartDate = "",
    studyEndDate = "",
    covariateSettings = covSettingsList
  )
  
  # create study pop args
  createStudyPopArgs1 <- CohortMethod::createCreateStudyPopulationArgs(
    firstExposureOnly = FALSE,
    restrictToCommonPeriod = FALSE,
    washoutPeriod = 0,
    removeDuplicateSubjects = "keep all",
    removeSubjectsWithPriorOutcome = TRUE,
    minDaysAtRisk = 1,
    riskWindowStart = 1,
    startAnchor = "cohort start",
    riskWindowEnd = 0,
    endAnchor = "cohort end",
    censorAtNewRiskWindow = FALSE
  )
  
  # create outcome model using covariates
  fitOutcomeModelArgs1 <- CohortMethod::createFitOutcomeModelArgs(
    modelType = "cox",
    useCovariates = TRUE
  )
  
  #bind settings
  cmAnalysis1 <- CohortMethod::createCmAnalysis(
    analysisId = 1,
    description = "Sodhi Replication",
    getDbCohortMethodDataArgs = getDbCmDataArgs1,
    createStudyPopArgs = createStudyPopArgs1,
    fitOutcomeModel = TRUE,
    fitOutcomeModelArgs = fitOutcomeModelArgs1
  )
  # turn into list
  cmAnalysisList <- list(cmAnalysis1)
  
  tcosList <- createTcos2()
  outcomesOfInterest <- getOutcomesOfInterest()
  
  # run Results
  results <- CohortMethod::runCmAnalyses(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    exposureDatabaseSchema = cohortDatabaseSchema,
    exposureTable = cohortTable,
    outcomeDatabaseSchema = cohortDatabaseSchema,
    outcomeTable = cohortTable,
    outputFolder = sodhiOutputFolder,
    tempEmulationSchema = tempEmulationSchema,
    cmAnalysisList = cmAnalysisList,
    targetComparatorOutcomesList = tcosList,
    getDbCohortMethodDataThreads = min(3, maxCores),
    createStudyPopThreads = min(3, maxCores),
    createPsThreads = max(1, round(maxCores / 10)),
    psCvThreads = min(10, maxCores),
    trimMatchStratifyThreads = min(10, maxCores),
    fitOutcomeModelThreads = max(1, round(maxCores / 4)),
    outcomeCvThreads = min(4, maxCores),
    refitPsForEveryOutcome = FALSE,
    outcomeIdsOfInterest = outcomesOfInterest
  )
  
  message("Summarizing results")
  analysisSummary <- CohortMethod::summarizeAnalyses(
    referenceTable = results,
    outputFolder = sodhiOutputFolder
  )
  
  analysisSummary <- addCohortNames(analysisSummary, "targetId", "targetName")
  analysisSummary <- addCohortNames(analysisSummary, "comparatorId", "comparatorName")
  analysisSummary <- addCohortNames(analysisSummary, "outcomeId", "outcomeName")
  analysisSummary <- addAnalysisDescription(analysisSummary, "analysisId", "analysisDescription")
  write.csv(analysisSummary, file.path(outputFolder, "analysisSummary_sodhi.csv"), row.names = FALSE)
  
}

#' Execute the Sodhi Replication
#'
#' @details
#' This function executes the ReproducibilitySodhi2023 Study replication.
#'
#' @param connectionDetails    An object of type \code{connectionDetails} as created using the
#'                             \code{\link[DatabaseConnector]{createConnectionDetails}} function in the
#'                             DatabaseConnector package.
#' @param cdmDatabaseSchema    Schema name where your patient-level data in OMOP CDM format resides.
#'                             Note that for SQL Server, this should include both the database and
#'                             schema name, for example 'cdm_data.dbo'.
#' @param cohortDatabaseSchema Schema name where intermediate data can be stored. You will need to have
#'                             write priviliges in this schema. Note that for SQL Server, this should
#'                             include both the database and schema name, for example 'cdm_data.dbo'.
#' @param cohortTable                  Name of the cohort table.
#' @param cohortInclusionTable         Name of the inclusion table, one of the tables for storing
#'                                     inclusion rule statistics.
#' @param cohortInclusionResultTable   Name of the inclusion result table, one of the tables for
#'                                     storing inclusion rule statistics.
#' @param cohortInclusionStatsTable    Name of the inclusion stats table, one of the tables for storing
#'                                     inclusion rule statistics.
#' @param cohortSummaryStatsTable      Name of the summary stats table, one of the tables for storing
#'                                     inclusion rule statistics.
#' @param cohortCensorStatsTable       Name of the censor stats table, one of the tables for storing
#'                                     inclusion rule statistics.
#' @param tempEmulationSchema Some database platforms like Oracle and Impala do not truly support temp tables. To
#'                            emulate temp tables, provide a schema with write privileges where temp tables
#'                            can be created.
#' @param outputFolder         Name of local folder to place results; make sure to use forward slashes
#'                             (/). Do not use a folder on a network drive since this greatly impacts
#'                             performance.
#' @param maxCores             How many parallel cores should be used? If more cores are made available
#'                             this can speed up the analyses.
#'
#' @export
#' 
#' 
executeSodhiReplication <- function(connectionDetails,
                                    cdmDatabaseSchema,
                                    cohortDatabaseSchema,
                                    cohortTable,
                                    cohortInclusionTable = paste0(cohortTable, "_inclusion"),
                                    cohortInclusionResultTable = paste0(cohortTable, "_inclusion_result"),
                                    cohortInclusionStatsTable = paste0(cohortTable, "_inclusion_stats"),
                                    cohortSummaryStatsTable = paste0(cohortTable, "_summary_stats"),
                                    cohortCensorStatsTable = paste0(cohortTable, "_censor_stats"),
                                    outputFolder,
                                    maxCores,
                                    oracleTempSchema = NULL,
                                    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")
                                    ) {
  
  ## Setup
  outputFolder <- normalizePath(outputFolder, mustWork = FALSE)
  if (!file.exists(outputFolder)) {
    dir.create(outputFolder, recursive = TRUE)
  }
  
  ParallelLogger::addDefaultFileLogger(file.path(outputFolder, "log.txt"))
  ParallelLogger::addDefaultErrorReportLogger(file.path(outputFolder, "errorReportR.txt"))
  on.exit(ParallelLogger::unregisterLogger("DEFAULT_FILE_LOGGER", silent = TRUE))
  on.exit(ParallelLogger::unregisterLogger("DEFAULT_ERRORREPORT_LOGGER", silent = TRUE), add = TRUE)
  
  if (!is.null(oracleTempSchema) && oracleTempSchema != "") {
    warning("The 'oracleTempSchema' argument is deprecated. Use 'tempEmulationSchema' instead.")
    tempEmulationSchema <- oracleTempSchema
  }
  if (connectionDetails$dbms %in% c("oracle", "bigquery", "impala", "spark") && is.null(tempEmulationSchema)) {
    stop(sprintf("DBMS '%s' requires 'tempEmulationSchema' to be set.", connectionDetails$dbms))
  }
  if (!is.null(getOption("andromedaTempFolder")) && !file.exists(getOption("andromedaTempFolder"))) {
    warning("andromedaTempFolder '", getOption("andromedaTempFolder"), "' not found. Attempting to create folder")
    dir.create(getOption("andromedaTempFolder"), recursive = TRUE)
  }
  
  ## Create Cohorts
  message("Creating exposure and outcome cohorts")
  createCohorts(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = list(
      cohortTable = cohortTable,
      cohortInclusionTable = cohortInclusionTable,
      cohortInclusionResultTable = cohortInclusionResultTable,
      cohortInclusionStatsTable = cohortInclusionStatsTable,
      cohortSummaryStatsTable = cohortSummaryStatsTable,
      cohortCensorStatsTable = cohortCensorStatsTable
    ),
    tempEmulationSchema = tempEmulationSchema,
    outputFolder = outputFolder
  )
  
  ## Run Sodhi Replication
  message("Running Sodhi Replication")
  runSodhiReplication(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    tempEmulationSchema = tempEmulationSchema,
    outputFolder = outputFolder,
    maxCores = maxCores
  )
  
  invisible(NULL)
}
