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