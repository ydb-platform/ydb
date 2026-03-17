/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#ifndef NCOFFSETS_H
#define NCOFFSETS_H 1

/* Define indices for every primitive C type */
/* NAT => NOT-A-TYPE*/
#define NC_NATINDEX       0
#define NC_CHARINDEX      1
#define NC_UCHARINDEX     2
#define NC_SHORTINDEX     3
#define NC_USHORTINDEX    4
#define NC_INTINDEX       5
#define NC_UINTINDEX      6
#define NC_LONGINDEX      7
#define NC_ULONGINDEX     8
#define NC_LONGLONGINDEX  9
#define NC_ULONGLONGINDEX 10
#define NC_FLOATINDEX     11
#define NC_DOUBLEINDEX    12
#define NC_PTRINDEX       13
#define NC_NCVLENINDEX    14

#define NC_NCTYPES        15

typedef struct NCalignment {
    char* type_name;
    size_t alignment;
} NCalignment;

typedef NCalignment NCtypealignvec;

/* Capture in struct and in a vector*/
typedef struct NCtypealignset {
    NCalignment charalign;	/* char*/
    NCalignment ucharalign;	/* unsigned char*/
    NCalignment shortalign;	/* short*/
    NCalignment ushortalign;	/* unsigned short*/
    NCalignment intalign;		/* int*/
    NCalignment uintalign;	/* unsigned int*/
    NCalignment longalign;	/* long*/
    NCalignment ulongalign;	/* unsigned long*/
    NCalignment longlongalign;	/* long long*/
    NCalignment ulonglongalign;	/* unsigned long long*/
    NCalignment floatalign;	/* float*/
    NCalignment doublealign;	/* double*/
    NCalignment ptralign;		/* void**/
    NCalignment ncvlenalign;	/* nc_vlen_t*/
} NCtypealignset;

EXTERNL int NC_class_alignment(int ncclass, size_t*);
EXTERNL void NC_compute_alignments(void);

/* From libdispatch/dinstance.c */
EXTERNL int NC_type_alignment(int ncid, nc_type xtype, size_t*);

/* From libdispatch/dinstance_intern.c */
/**
 * Internal version of NC_type_alignment
 */
struct NC_FILE_INFO; /* forward */
struct NC_TYPE_INFO; /* forward */

EXTERNL int NC_type_alignment_internal(struct NC_FILE_INFO* file, nc_type xtype, struct NC_TYPE_INFO* utype, size_t* alignp);
EXTERNL uintptr_t NC_read_align(uintptr_t addr, size_t alignment);

#endif /*NCOFFSETS_H*/
