/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT file for more information. */

#ifndef OCUTIL_H
#define OCUTIL_H 1

/* Forward */
struct OCstate;

#define ocmax(x,y) ((x) > (y) ? (x) : (y))

extern char* ocstrndup(const char* s, size_t len);
extern int ocstrncmp(const char* s1, const char* s2, size_t len);

extern int occopycat(char* dst, size_t size, size_t n, ...);
extern int occoncat(char* dst, size_t size, size_t n, ...);

extern size_t octypesize(OCtype etype);
extern char*  octypetostring(OCtype octype);
extern char*  octypetoddsstring(OCtype octype);
extern char* ocerrstring(int err);
extern OCerror ocsvcerrordata(struct OCstate*,char**,char**,long*);
extern OCerror octypeprint(OCtype etype, void* value, size_t bufsize, char* buf);
extern size_t xxdrsize(OCtype etype);

extern int oc_ispacked(OCnode* node);

extern size_t octotaldimsize(size_t,size_t*);

extern size_t ocarrayoffset(size_t rank, size_t*, const size_t*);
extern void ocarrayindices(size_t index, size_t rank, size_t*, size_t*);
extern size_t ocedgeoffset(size_t rank, size_t*, size_t*);

extern int ocvalidateindices(size_t rank, size_t*, size_t*);

extern void ocmakedimlist(NClist* path, NClist* dims);

extern int ocfindbod(NCbytes* buffer, size_t*, size_t*);

/* Reclaimers*/
extern void ocfreeprojectionclause(OCprojectionclause* clause);

/* Misc. */

/* merge two envv style lists */
extern char** ocmerge(const char** list1, const char** list2);

extern int ocmktmp(const char* base, char** tmpnamep);

extern void ocdataddsmsg(struct OCstate*, struct OCtree*);

extern char* ocdtmodestring(OCDT mode,int compact);

/* Define some classifiers */
#define ociscontainer(t) ((t) == OC_Dataset || (t) == OC_Structure || (t) == OC_Sequence || (t) == OC_Grid || (t) == OC_Attributeset)

#define ocisatomic(t) ((t) == OC_Atomic)

#endif /*UTIL_H*/
