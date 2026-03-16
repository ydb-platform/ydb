/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/
#ifndef DUMP_H
#define DUMP_H

typedef struct Dimschema {
    int dimid;
/*    int cloneid;*/
    size_t size;
    char name[NC_MAX_NAME+1];
} Dim;

typedef struct Varschema {
    int varid;
/*    int cloneid;*/
    char name[NC_MAX_NAME+1];
    nc_type nctype;
    int ndims;
    int dimids[NC_MAX_VAR_DIMS];
    size_t nelems; /*# elements*/
    size_t alloc; /* malloc size*/
    int natts;
    NCattribute* atts;
} Var;

typedef struct NChdr {
    int ncid;
    int format;
    int ndims;
    int nvars;
    int ngatts;
    int unlimid; /* id of the (1) unlimited dimension*/
    Dim* dims;
    Var* vars;
    NCattribute* gatts;
    NCbytes* content;
} NChdr;

extern int dumpmetadata(int ncid, NChdr**);
extern void dumpdata1(nc_type nctype, size_t index, char* data);

extern char* dumppath(struct CDFnode* node);
extern char* dumptree(CDFnode* root);
extern char* dumpvisible(CDFnode* root);
extern char* dumpnode(CDFnode* node);

extern char* dumpalign(struct NCD2alignment*);

extern char* dumpcachenode(NCcachenode* node);
extern char* dumpcache(NCcache* cache);

extern int dumpmetadata(int ncid, NChdr** hdrp);
extern void dumpdata1(nc_type nctype, size_t index, char* data);
extern char* dumpprojections(NClist* projections);
extern char* dumpprojection(DCEprojection* proj);
extern char* dumpselections(NClist* selections);
extern char* dumpselection(DCEselection* sel);
extern char* dumpconstraint(DCEconstraint* con);
extern char* dumpsegments(NClist* segments);
extern char* dumpslice(DCEslice* slice);
extern char* dumpslices(DCEslice* slice, unsigned int rank);

extern void dumpraw(void*);
extern void dumplistraw(NClist*);

#endif /*DUMP_H*/

