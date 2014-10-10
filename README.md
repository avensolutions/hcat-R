# hcat-R

HCatalog Functions for R

## Features

* List and describe available HCat objects (databases and tables)
* Get HDFS directory locations for HCat tables

## Installation

sudo R CMD INSTALL hcat_x.x.tar.gz

### Dependecies

R (>= 3.0.0)  
stringr  

## Usage Example

	> require(hcat)
	Loading required package: hcat
	Loading required package: stringr
	> hcat.showdatabases()
	[1] "default"
	> hcat.showtables()
	[1] "testtable"
	> hcat.gettabledirectory("testtable")
	[1] "hdfs://hdpmaster1.hdp.hadoop:8020/apps/hive/warehouse/testtable"
	> hcat.describe("mortdata")
	       colname coltype coldesc
	1           id      int    None
	2  creditscore     	int    None
	3     houseage     	int    None
	4  yearsemploy     	int    None
	5       ccdebt     	int    None
	6         year     	int    None
	7      default     	int    None



