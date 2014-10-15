#' hcat.showdatabases
#'
#' Returns a list of HCatalog Databases
#' @return List of HCatalog Databases
#' @usage hcat.showdatabases()
hcat.showdatabases <- function(){
	res <- system("hcat -e 'SHOW DATABASES'", intern=TRUE, ignore.stderr=TRUE)
	res
}

#' hcat.showtables
#'
#' Returns a list of HCatalog Tables for a Database
#' @return List of HCatalog Tables
#' @usage hcat.showtables(database)
hcat.showtables <- function(database){
	if (missing(database))
		database = "default"
	res <- system(paste("hcat -e 'USE ", database, ";SHOW TABLES'"), intern=TRUE, ignore.stderr=TRUE)
	res
}

#' hcat.describe
#'
#' Returns a shcema for a HCatalog Table
#' @return Schema Definition for HCatalog Tables
#' @usage hcat.describe(tablename, database="default")
hcat.describe <- function(tablename, database="default"){
	if (missing(tablename))
		stop("Need to specify HCat table.")
	res <- system(paste("hcat -e 'USE ", database, ";DESCRIBE ", tablename, "'"), intern=TRUE, ignore.stderr=TRUE)
	colname <- c()
	coltype <- c()
	coldesc <- c()
	for (i in 1:length(res)) {
		collist <- strsplit(res[i], "\t")[[1]]
		colname <- c(colname, str_trim(collist[1], side = "both"))
		coltype <- c(coltype, str_trim(collist[2], side = "both"))
		coldesc <- c(coldesc, str_trim(collist[3], side = "both"))
	}
	coldata <- data.frame(colname, coltype, coldesc)
	coldata
}

#' hcat.gettabledirectory
#'
#' Returns the HDFS directory for a HCatalog Table
#' @return HFDS Directory for a HCatalog Table
#' @usage hcat.gettabledirectory(tablename, database="default")
hcat.gettabledirectory  <- function(tablename, database="default"){
	if (missing(tablename))
		stop("Need to specify HCat table.")
	res <- system(paste("hcat -e 'USE ", database, ";DESCRIBE FORMATTED ", tablename, "'"), intern=TRUE, ignore.stderr=TRUE)
	for (i in 1:length(res)) {
		if(str_detect(res[i], "Location: ")) {
		locstrarray <- strsplit(res[i], "\t")[[1]]
		}
	}
	locstrarray[2]
}

#' hcat.execute
#'
#' Executes a HQL Query against an object in HCatalog
#' @return Results as 
#'		a data frame if no output directory is specified; or
#'		writes results to a file in HDFS in the specified output directory with the specified fieldDelimiter
#' @usage hcat.execute(hqlQuery, database="default", outputDir="", fieldDelimiter="\t")
hcat.execute <- function(hqlQuery, database="default", outputDir="", fieldDelimiter="\\t"){
  # create temp view for output schema
  tmpViewName <- paste("tmp_", gsub("\\.", "", as.numeric(Sys.time())), sep="")
  viewCreateSql <- paste("hive -e 'USE ", database, "; CREATE VIEW ", tmpViewName, " AS ", hqlQuery, "'", sep="")                         
  system(viewCreateSql, intern=TRUE, ignore.stderr=TRUE)
  if (identical(outputDir, "")) {
    # no output dir selected prepare data frame for query results
    # get output schema
    coldata <- hcat.describe(tmpViewName, database)
    cols <- coldata[1:1]
    collist <- ""
    for(i in 1:nrow(cols)) {
      row <- cols[i,]
      collist  <- paste(collist, row)
    }
    collist <- sub("^\\s+", "", collist)
    collistc <- strsplit(collist, " ")[[1]]
    # drop temp view
    viewDropSql <- paste("hive -e 'USE ", database, "; DROP VIEW ", tmpViewName, "'", sep="")                 
    system(viewDropSql, intern=TRUE, ignore.stderr=TRUE)
    #
    sqlQuery <- paste("hive -e 'USE ", database, ";", hqlQuery, "'", sep="")
    res <- system(sqlQuery, intern=TRUE, ignore.stderr=TRUE)
    outframe <- read.table(textConnection(res), sep="\t", col.names=collistc)
  } else {
    # write query results to designated output directory
    # create temp external table
    tmpTableName <- paste(tmpViewName, "_EXT", sep="")
    coldata <- hcat.describe(tmpViewName, database)
    collist <- "("
    lstRetCols <- list()
    for(i in 1:nrow(coldata)) {
      colname <- gsub("_", "", coldata[i,1])
      coltype <- coldata[i,2]
      collist  <- paste(collist, colname, coltype, ",")
      lst <- list(index = i, newName = as.character(coldata$colname[i]))
      lstRetCols[[i]] <- lst
    }
    collist <- sub("^\\s+", "", collist)
    collist <- sub("..$","", collist)
    collist <- paste(collist, ")")
    createTableDDL <- paste("CREATE EXTERNAL TABLE ", tmpTableName, collist, " ROW FORMAT DELIMITED FIELDS TERMINATED BY '", fieldDelimiter, "' STORED AS TEXTFILE LOCATION '", outputDir, "'", sep="")
    sqlQuery <- paste('hive -e "USE ', database, ";", createTableDDL, '"', sep="")
    system(sqlQuery, intern=TRUE, ignore.stderr=TRUE)
    # insert results into external table
    sqlQuery <- paste("hive -e \"USE ", database, "; INSERT INTO TABLE ", tmpTableName, " ", hqlQuery, "\"", sep="")
    system(sqlQuery, intern=TRUE, ignore.stderr=TRUE)
    # drop temp table
    tableDropSql <- paste("hive -e \"USE ", database, "; DROP TABLE ", tmpTableName, "\"", sep="")                        
    system(tableDropSql, intern=TRUE, ignore.stderr=TRUE)
    # drop view
    viewDropSql <- paste("hive -e \"USE ", database, "; DROP VIEW ", tmpViewName, "\"", sep="")                           
    system(viewDropSql, intern=TRUE, ignore.stderr=TRUE)
    
    lstRetCols
  }
}