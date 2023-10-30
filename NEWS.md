ReproducibilitySodhi2023 v0.1.0
======================================

* Remove analysis 2 from master branch
* Add new outcome definitions for gastroparesis
* Add post 2020 comparison

ReproducibilitySodhi2023 v0.0.2
======================================

* Exclude covariates causing correlation error Issue #7

ReproducibilitySodhi2023 v0.0.1
======================================

Changes:

1. Using `CohortGenerator` package to generate cohorts. This does mean several other tables will be created to hold cohort creation statistics.

2. Checking whether `tempEmulationSchema` is provided for those DBMSs that need it.

3. Checking if `andromedaTempFolder` can be created.

4. Fetching vocabulary version, study package version, and observation period range when exporting database meta-data.

5. Using new `ParallelLogger`, so changing all `ParallelLogger::logInfo()` to `message()` for prettier code.

6. Normalizing all paths, so there should be no issue when relative paths are used.

7. Using `keyring` in example code.

8. Updating `renv.lock` to latest versions of all dependencies.

9. Storing cohort definitions according to new standard: JSON and SQL file names derived from cohort IDs. List of cohorts in `Cohorts.csv`, with columns `webApiCohortId`, `cohortId` and `cohortName`.



