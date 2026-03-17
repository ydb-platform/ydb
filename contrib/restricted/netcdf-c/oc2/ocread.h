/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT file for more information. */

#ifndef READ_H
#define READ_H


extern int readDDS(OCstate*, OCtree*, OCflags);
extern int readDAS(OCstate*, OCtree*, OCflags);

extern int readDATADDS(OCstate*, OCtree*, OCflags);

#endif /*READ_H*/
