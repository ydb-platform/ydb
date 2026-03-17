/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/
#ifndef DAPUTIL_H
#define DAPUTIL_H 1

/* Define a set of flags to control path construction */
#define PATHNC    1 /*Use ->ncname*/
#define PATHELIDE 2 /*Leave out elided nodes*/

/* mnemonic */
#define WITHDATASET 1
#define WITHOUTDATASET 0

/* sigh!, Forwards */
struct CDFnode;
struct NCTMODEL;
struct NCDAPCOMMON;

extern nc_type nctypeconvert(struct NCDAPCOMMON*, nc_type);
extern nc_type octypetonc(OCtype);
extern OCtype nctypetodap(nc_type);
extern size_t nctypesizeof(nc_type);
extern char* nctypetostring(nc_type);
extern char* maketmppath(char* path, char* prefix);

extern void collectnodepath(struct CDFnode*, NClist* path, int dataset);
extern void collectocpath(OClink conn, OCobject node, NClist* path);

extern char* makecdfpathstring(struct CDFnode*,const char*);
extern void clonenodenamepath(struct CDFnode*, NClist*, int);
extern char* makepathstring(NClist* path, const char* separator, int flags);

extern char* makeocpathstring(OClink, OCobject, const char*);

extern char* cdflegalname(char* dapname);

/* Given a param string; return its value or null if not found*/
extern const char* dapparamvalue(struct NCDAPCOMMON* drno, const char* param);
/* Given a param string; check for a given substring */
extern int dapparamcheck(struct NCDAPCOMMON* drno, const char* param, const char* substring);

extern int nclistconcat(NClist* l1, NClist* l2);
extern int nclistminus(NClist* l1, NClist* l2);
extern int nclistdeleteall(NClist* l1, void*);

extern char* getvaraprint(void* gv);

extern int dapinsequence(struct CDFnode* node);
extern int dapinstructarray(struct CDFnode* node);
extern int daptopgrid(struct CDFnode* node);
extern int daptopseq(struct CDFnode* node);
extern int daptoplevel(struct CDFnode* node);
extern int dapgridmap(struct CDFnode* node);
extern int dapgridarray(struct CDFnode* node);
extern int dapgridelement(struct CDFnode* node);

extern unsigned int modeldecode(int, const char*, const struct NCTMODEL*, unsigned int);
extern unsigned long getlimitnumber(const char* limit);

extern void dapexpandescapes(char *termstring);

/* Only used by libncdap4 */
extern int dapalignbuffer(NCbytes*, int alignment);
extern size_t dapdimproduct(NClist* dimensions);

/* Provide a wrapper for oc_fetch so we can log what it does */
extern NCerror dap_fetch(struct NCDAPCOMMON*,OClink,const char*,OCdxd,OCobject*);

extern int dap_badname(char* name);
extern char* dap_repairname(char* name);
extern char* dap_getselection(NCURI* uri);

extern int dapparamparselist(const char* s0, int delim, NClist* list);

#endif /*DAPUTIL_H*/
