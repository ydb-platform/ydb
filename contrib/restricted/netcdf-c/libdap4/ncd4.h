/*********************************************************************
  *   Copyright 2018, UCAR/Unidata
  *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
  *********************************************************************/

/*
External functions and state variables are
defined here, including function-like #defines.
*/

#ifndef NCD4_H
#define NCD4_H 1

#include "d4includes.h"
#include "d4util.h"
#include "d4debug.h"
#include "nc4internal.h"

/**************************************************/
/* Constants */

#define RCFILEENV "DAPRCFILE"

/* Figure out a usable max path name max */
#ifdef PATH_MAX /* *nix* */
#define NC_MAX_PATH PATH_MAX
#else
#  ifdef MAX_PATH /*windows*/
#    define NC_MAX_PATH MAX_PATH
#  else
#    define NC_MAX_PATH 4096
#  endif
#endif

#define COUNTERTYPE unsigned long long
#define COUNTERSIZE sizeof(COUNTERTYPE)

/* Clear allocated memory (see e.g. d4data.c); Potentially costly*/
#undef CLEARMEM

/* Clear empty structure alignment space; Potentially costly, but probably less than  CLEARMEM */
#define CLEARSTRUCT

/* Always use fixed size opaques */
#define FIXEDOPAQUE
#define DFALTOPAQUESIZE 16

/* Size of a chunk header */
#define CHUNKHDRSIZE 4

/* Special attributes */
#define D4CHECKSUMATTR "_DAP4_Checksum_CRC32"
#define D4LEATTR "_DAP4_Little_Endian" 

/**************************************************/

#undef nullfree
#ifndef nullfree
#define nullfree(m) ((m)==NULL?NULL:(free(m),NULL))
#endif
#ifndef nulldup 
#define nulldup(s) ((s)==NULL?NULL:strdup(s))
#endif
/**************************************************/
/* DSP API wrappers */

#ifdef FIX
EXTERNL int dsp_getDMR(ND4dsp* dsp, DCR** dcrp);
EXTERNL int dsp_getDAP(ND4dsp* dsp, DCR** dcrp);
EXTERNL int dsp_close(ND4dsp* dsp);

/* DSP API */
EXTERNL int dsp_open(const char* path, ND4dsp** dspp);

#endif

/**************************************************/

/* From d4http.c */
EXTERNL long NCD4_fetchhttpcode(CURL* curl);
EXTERNL int NCD4_fetchurl(CURL* curl, const char* url, NCbytes* buf, long* filetime, int* httpcode);
EXTERNL int NCD4_curlopen(CURL** curlp);
EXTERNL void NCD4_curlclose(CURL* curl);
EXTERNL int NCD4_fetchlastmodified(CURL* curl, char* url, long* filetime);
EXTERNL int NCD4_ping(const char* url);

/* From d4read.c */
EXTERNL int NCD4_readDMR(NCD4INFO* state, NCURI* url, NCD4response*);
EXTERNL int NCD4_readDAP(NCD4INFO* state, NCURI* ceuri, NCD4response*);
EXTERNL int NCD4_seterrormessage(NCD4response*, size_t len, char* msg);

/* From d4parser.c */
EXTERNL int NCD4_parse(NCD4meta*, NCD4response*, int dapparse);
EXTERNL NCD4node* NCD4_findAttr(NCD4node* container, const char* attrname);
EXTERNL NCD4node* NCD4_groupFor(NCD4node* node);
EXTERNL int NCD4_defineattr(NCD4meta* meta, NCD4node* parent, const char* aname, const char* typename, NCD4node** attrp);
EXTERNL void NCD4_setdebuglevel(NCD4parser*,int);

/* From d4printer.c */
EXTERNL int NCD4_print(NCD4meta*, NCbytes* output);

/* From d4meta.c */
EXTERNL NCD4meta* NCD4_newmeta(NCD4INFO*);
EXTERNL void NCD4_attachraw(NCD4meta*, size_t size, void* rawdata);
EXTERNL void NCD4_reclaimMeta(NCD4meta*);
EXTERNL void NCD4_resetMeta(NCD4meta*);
EXTERNL void reclaimNode(NCD4node* node);
EXTERNL int NCD4_metabuild(NCD4meta*, int ncid);
EXTERNL size_t NCD4_computeTypeSize(NCD4meta*, NCD4node* type);
EXTERNL int NCD4_findvar(NC* ncp, int ncid, int varid, NCD4node** varp, NCD4node** grpp);
EXTERNL char* NCD4_getVarFQN(NCD4node* var, const char* tail);


/* From d4chunk.c */
EXTERNL int NCD4_dechunk(NCD4response*);
EXTERNL int NCD4_infermode(NCD4response*);
struct NCD4serial;
EXTERNL void NCD4_resetSerial(struct NCD4serial* serial, size_t rawsize, void* rawdata);
EXTERNL void NCD4_moveSerial(struct NCD4serial* serial, struct NCD4serial* dst);

/* From d4swap.c */
EXTERNL int NCD4_swapdata(NCD4response*, NCD4node* topvar, int doswap);

/* From d4fix.c */
EXTERNL int NCD4_delimit(NCD4meta*, NCD4node* var, NCD4offset* offset, int inferredchecksumming);
EXTERNL int NCD4_moveto(NCD4meta*, NCD4node* var, d4size_t count, NCD4offset* offset);
EXTERNL int NCD4_toposort(NCD4meta*);

/* From d4data.c */
EXTERNL int NCD4_parcelvars(NCD4meta* meta, NCD4response* resp);
EXTERNL int NCD4_processdata(NCD4meta*,NCD4response*);
EXTERNL int NCD4_movetoinstance(NCD4meta*, NCD4node* type, NCD4offset* offset, void** dstp, NClist* blobs);
EXTERNL int NCD4_getToplevelVars(NCD4meta* meta, NCD4node* group, NClist* toplevel);
EXTERNL int NCD4_inferChecksums(NCD4meta* meta, NCD4response* resp);

/* From d4util.c */
EXTERNL d4size_t NCD4_dimproduct(NCD4node* node);
EXTERNL size_t NCD4_typesize(nc_type tid);
EXTERNL int NCD4_isLittleEndian(void);/* Return 1 if this machine is little endian */
EXTERNL int NCD4_errorNC(int code, const int line, const char* file);
EXTERNL int NCD4_error(int code, const int line, const char* file, const char* fmt, ...);
EXTERNL char* NCD4_makeFQN(NCD4node* node);
EXTERNL char* NCD4_makeName(NCD4node*,const char* sep);
EXTERNL int NCD4_parseFQN(const char* fqn0, NClist* pieces);
EXTERNL char* NCD4_deescape(const char* esc);
EXTERNL char* NCD4_entityescape(const char* s);
EXTERNL size_t NCD4_elidenuls(char* s, size_t slen);
EXTERNL void* NCD4_getheader(void* p, NCD4HDR* hdr, int hostlittleendian);
EXTERNL void NCD4_reporterror(NCD4response*, NCURI* uri);

/* From d4dump.c */
EXTERNL void NCD4_dumpbytes(size_t size, const void* data0, int swap);
EXTERNL void NCD4_tagdump(size_t size, const void* data0, int swap, const char* tag);
EXTERNL void NCD4_dumpvars(NCD4node* group);
EXTERNL union ATOMICS* NCD4_dumpatomic(NCD4node* var, void* data);

/* From d4rc.c */
EXTERNL int NCD4_rcload(void);
EXTERNL int NCD4_rcprocess(NCD4INFO* info);
EXTERNL void NCD4_rcfree(NClist* rc);
EXTERNL char* NCD4_rclookup(char* key, char* hostport);
EXTERNL int NCD4_parseproxy(NCD4INFO* info, const char* surl);
EXTERNL int NCD4_rcdefault(NCD4INFO*);

/* From d4cvt.c */
EXTERNL int NCD4_convert(nc_type srctype, nc_type dsttype, char* memory0, char* value0, size_t count);

/* d4file.c */
EXTERNL int NCD4_readDMRorDAP(NCD4INFO* d4info, NCD4mode mode);
EXTERNL void NCD4_applyclientfragmentcontrols(NCD4INFO* d4info);
EXTERNL void NCD4_applychecksumcontrols(NCD4INFO* d4info, NCD4response*);
EXTERNL int NCD4_newInfo(NCD4INFO** d4infop);
EXTERNL void NCD4_reclaimInfo(NCD4INFO* d4info);
EXTERNL void NCD4_resetInfoforRead(NCD4INFO* d4info);
EXTERNL int NCD4_newResponse(NCD4INFO*,NCD4response** respp);
EXTERNL void NCD4_reclaimResponse(NCD4response* d4resp);
EXTERNL void NCD4_resetInfoForRead(NCD4INFO* d4info);
EXTERNL int NCD4_newMeta(NCD4INFO*,NCD4meta**);
EXTERNL void NCD4_reclaimMeta(NCD4meta*);

/* ncd4dispatch.c */
struct NC_reservedatt; /*forward*/
EXTERNL const struct NC_reservedatt* NCD4_lookupreserved(const char* name);

/* Add an extra function whose sole purpose is to allow
   configure(.ac) to test for the presence of this code.
*/
EXTERNL int nc__dap4(void);

/**************************************************/
/* Macro defined functions */

#undef NCCHECK
#undef FAIL
#define NCCHECK(expr) if((ret=(expr))) {ret = NCD4_errorNC(ret,__LINE__,__FILE__); goto done;}else{}
#define FAIL(code,fmt,...) do{ret=NCD4_error(code,__LINE__,__FILE__,fmt , ##__VA_ARGS__); goto done;}while(0)

#undef BUILDOFFSET
#define BUILDOFFSET(base,size) NCD4_buildoffset(base,size)
EXTERNL NCD4offset* NCD4_buildoffset(void* base, d4size_t size);

#undef INCR
#undef DECR
#undef DELTA
#if 0
#define INCR(offset,size) ((void*)(((char*)(offset->offset))+(size)))
#define DECR(offset,size) ((void*)(((char*)(offset->offset))-(size)))
#define DELTA(p1,p2) ((ptrdiff_t)(((char*)(p1))-((char*)(p2))))
#endif
EXTERNL void NCD4_incr(NCD4offset* p, d4size_t size);
EXTERNL void NCD4_decr(NCD4offset* p, d4size_t size);
#define INCR(offset,size) NCD4_incr(offset,size)
#define DECR(offset,size) NCD4_decr(offset,size)
#define OFFSETSIZE(p,mark) ((d4size_t)(((ptrdiff_t)(p)->offset) - ((ptrdiff_t)(mark))))
#define TRANSFER(dst,src,size) memcpy((dst),(src)->offset,size)
#define DELTA(p1,p2) ((ptrdiff_t)(((char*)(p1))-((char*)(p2))))
#define MARK(p,mark) do {(mark) = (p)->offset;} while(0)

#undef GETCOUNTER
#undef SKIPCOUNTER

/* Unclear which macros are defined for which compilers.
   see: https://sourceforge.net/p/predef/wiki/Architectures/
*/
#if 0
#if defined(__arm__) && __ARM_ARCH < 8
EXTERNL d4size_t NCD4_getcounter(void* p);
#define GETCOUNTER(p) NCD4_getcounter(p)
#else
#define GETCOUNTER(p) ((d4size_t)*((COUNTERTYPE*)(p)))
#endif
#endif /*0*/
EXTERNL d4size_t NCD4_getcounter(NCD4offset* p);
#define GETCOUNTER(p) NCD4_getcounter(p)

#define SKIPCOUNTER(p) INCR(p,COUNTERSIZE)

#undef PUSH
#define PUSH(list,value) do{if((list)==NULL) {(list)=nclistnew();} else{}; nclistpush((list),(value));}while(0)

#define getnc3id(d4) (getdap(d4)->nc4id)

#define ISTOPLEVEL(var) ((var)->container == NULL || (var)->container->sort == NCD4_GROUP)

#define FILEIDPART(NCID) (((unsigned int) (NCID)) >> ID_SHIFT)
#define GROUPIDPART(NCID) (((unsigned int) (NCID)) & GRP_ID_MASK)
#define MAKENCID(grp,file) ((((unsigned int)(file)) << ID_SHIFT) | (grp))

#define getdap(ncp) ((NCD4INFO*)((NC*)ncp)->dispatchdata)
#define getnc4id(ncp) (getdap(ncp)->substrate.nc4id)

/* Convert a dap4 grpid to a substrate id */
#define makenc4id(ncp,dap4id) (((dap4id) & GRP_ID_MASK) | getdap(ncp)->substrate.nc4id)
/* and the inverse */
#define makedap4id(ncp,nc4id) (((nc4id) & GRP_ID_MASK) | (ncp)->ext_ncid)

#ifdef CLEARMEM
#define d4alloc(n) (calloc(1,(size_t)(n)))
#else
#define d4alloc(n) (malloc((size_t)(n)))
#endif

/* A number of hacks have been inserted
   to deal with issues in accessing hyrax
   using DAP4.
*/
#define HYRAXHACK

EXTERNL NC* NCD4_get_substrate_nc(NC* nc);

#endif /*NCD4_H*/

