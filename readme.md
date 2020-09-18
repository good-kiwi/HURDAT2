# Introduction 
Historical information about cyclones is published by the NHC (www.nhc.noaa.gov) in plain text/csv files called HURDAT2. 
These files contain a lot of information, but the format makes it difficult to filter and query as there are two 
different record types in the file. This script splits the two different record types into two relational tables stored
in a sqlserver database with appropriate keys between the tables as well as spatial indexes to boost performance of
spatial queries.

# Getting Started
1. Installation process: This script was tested in python 3.8. In order to run this on your machine, simply download
the script. There will be 3 items that need to be updated in the script to run on your system.
    1. database name
    1. location of hurdat2 txt files. My default is to store them in a resources sub folder of the script, but I did not
include them here because I do not have a license for them. They can be downloaded here 
(https://www.nhc.noaa.gov/data/#hurdat). There are currently 2 files, one for the pacific and one for the atlantic.
    1. open a terminal window and navigate to the download directory. Type ```python HURDAT2``` to run the script
1. Software dependencies. This script requires pandas 1.1.2 and sqlalchemy 1.3.19

# Contribute
* Currently there are 4 invalid codes found in the pacific file. These are currently assigned null values. It would be 
great if these errors could be fixed.
* It might be useful to have the underlying pandas datasets be made available if a user wanted to import this script 
into their own code.
* Example queries of the resulting database
* Add functions to calculate interpolated values along the path.
