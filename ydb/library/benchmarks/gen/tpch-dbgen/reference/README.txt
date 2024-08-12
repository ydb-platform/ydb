/*
 * $Id: README.txt,v 1.4 2006/08/07 17:36:46 jms Exp $
 */

1. Goal
This document summarizes the steps required to validate the data set generated
by DBGEN, relative to the reference data published by the TPC. 

2. Pre-Requesites
  Toolset executable: This readme file is included as part of the toolset
  distributed by the TPC. The DBGEN and QGEN executables must be built from 
  the same distribution.

  Reference Data Set(s): For each permissible scale factor, the TPC has
  produced a separate zip file that defines the reference point for the base 
  data set, the update data set, and the output of QGEN. These must be 
  downloaded separately from the TPC web site. 

3. Validation Process

a. Setup
  The commands that are included in this distribution assume that they are
  being executed from <DBGEN>/reference, where <DBGEN> is directory into which
  the DBGEN distribution was unpacked. If that is not the case for your
  environemnt, the paths to executables and associated files will need to be
  edited to match your directory structure.
 
b. Base Data Validation
  The base data set is produced using cmd_base_sf<n> where <n> is the scale 
  factor to be generated. The resulting files will be produced in the current 
  working directory.  The generated files will be of the form <name>.tbl.<nnn>,
  where <name> will is the name of one of the tables in the TPCH schema, and 
  <nnn> identifies a particular data generation step.

  The file set produced by genbaserefdata.sh should match the <name>.tbl.<nnn> 
  files found in the reference data set for the same scale factor.

c. Update Data Validation
  The update data set is produced using cmd_update_sf<n> where <n> is the scale 
  factor to be generated. The resulting files will be produced in the current 
  working directory.  The generated files will be of the form 
  <name>.tbl.u<mmm>.<nnn> and delete.u<nnn>.<mmm>, where <name> will is the 
  name of one of the tables in the TPCH schema, and <mmm> and <nnn> identify 
  a particular data generation step.

  For each file produced by DBGEN, the first 100 lines of the file should
  match the delete.tbl.u<nnn>.<mmm> and <name>.tbl.u<nnn>.<mmm> files found in the
  reference data set. Where the files from the reference data set contain
  fewer than 100 lines, the file should match in their entriety.

d. QGEN Parameter Generation
  The qgen paramter sets are produced using the cmd_qgen_sf<n> files, where
  <n> matches the scale factor being validated.  The resulting files will
  be produced in the current working directory. The generated files will be of
  the form subparam_<nnn>, where <nnn> identifies a particular qgen subsituttion
  set.

  The file set produced by cmd_qgen_<sf> script should match the subparam.<nnn>
  files found in the reference data set.

/*********************
* Revision History
* ===================
* $Log: README.txt,v $
* Revision 1.4  2006/08/07 17:36:46  jms
* typo corrections
*
* Revision 1.3  2006/03/30 22:12:36  jms
* reduce to simple scripts
*
