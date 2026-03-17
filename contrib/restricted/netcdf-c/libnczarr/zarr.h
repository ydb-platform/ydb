/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

/**
 * Provide the zarr specific code to implement the netcdf-4 code.
 *
 * @author Dennis Heimbigner
 */

#ifndef ZARR_H
#define ZARR_H

struct ChunkKey;
struct S3credentials;

/* Intermediate results */
struct ZCVT {
    signed long long int64v;
    unsigned long long uint64v;
    double float64v;
    char* strv; /* null terminated utf-8 */
};

#define zcvt_empty {0,0,0.0,NULL}

/* zarr.c */
EXTERNL int ncz_create_dataset(NC_FILE_INFO_T*, NC_GRP_INFO_T*, NClist* controls);
EXTERNL int ncz_open_dataset(NC_FILE_INFO_T*, NClist* controls);
EXTERNL int ncz_del_attr(NC_FILE_INFO_T* file, NC_OBJ* container, const char* name);

/* HDF5 Mimics */
EXTERNL int NCZ_isnetcdf4(struct NC_FILE_INFO*);
EXTERNL int NCZ_get_libversion(unsigned long* majorp, unsigned long* minorp,unsigned long* releasep);
EXTERNL int NCZ_get_superblock(NC_FILE_INFO_T* file, int* superblockp);

EXTERNL int ncz_unload_jatts(NCZ_FILE_INFO_T*, NC_OBJ* container, NCjson* jattrs, NCjson* jtypes);

/* zclose.c */
EXTERNL int ncz_close_file(NC_FILE_INFO_T* file, int abort);

/* zcvt.c */
EXTERNL int NCZ_json2cvt(const NCjson* jsrc, struct ZCVT* zcvt, nc_type* typeidp);
EXTERNL int NCZ_convert1(const NCjson* jsrc, nc_type, NCbytes*);
EXTERNL int NCZ_stringconvert1(nc_type typid, char* src, NCjson* jvalue);
EXTERNL int NCZ_stringconvert(nc_type typid, size_t len, void* data0, NCjson** jdatap);

/* zsync.c */
EXTERNL int ncz_sync_file(NC_FILE_INFO_T* file, int isclose);
EXTERNL int ncz_sync_grp(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp, int isclose);
EXTERNL int ncz_sync_atts(NC_FILE_INFO_T*, NC_OBJ* container, NCindex* attlist, NCjson* jatts, NCjson* jtypes, int isclose);
EXTERNL int ncz_read_grp(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp);
EXTERNL int ncz_read_atts(NC_FILE_INFO_T* file, NC_OBJ* container);
EXTERNL int ncz_read_vars(NC_FILE_INFO_T* file, NC_GRP_INFO_T* grp);
EXTERNL int ncz_read_file(NC_FILE_INFO_T* file);
EXTERNL int ncz_write_var(NC_VAR_INFO_T* var);
EXTERNL int ncz_read_superblock(NC_FILE_INFO_T* zinfo, char** nczarrvp, char** zarrfp);

/* zutil.c */
EXTERNL int NCZ_grpkey(const NC_GRP_INFO_T* grp, char** pathp);
EXTERNL int NCZ_varkey(const NC_VAR_INFO_T* var, char** pathp);
EXTERNL int NCZ_dimkey(const NC_DIM_INFO_T* dim, char** pathp);
EXTERNL int ncz_splitkey(const char* path, NClist* segments);
EXTERNL int ncz_nctypedecode(const char* snctype, nc_type* nctypep);
EXTERNL int ncz_nctype2dtype(nc_type nctype, int endianness, int purezarr,int len, char** dnamep);
EXTERNL int ncz_dtype2nctype(const char* dtype, nc_type typehint, int purezarr, nc_type* nctypep, int* endianp, int* typelenp);
EXTERNL int NCZ_inferattrtype(const NCjson* value, nc_type typehint, nc_type* typeidp);
EXTERNL int NCZ_inferinttype(unsigned long long u64, int negative);
EXTERNL int ncz_fill_value_sort(nc_type nctype, int*);
EXTERNL int NCZ_createobject(NCZMAP* zmap, const char* key, size64_t size);
EXTERNL int NCZ_uploadjson(NCZMAP* zmap, const char* key, const NCjson* json);
EXTERNL int NCZ_downloadjson(NCZMAP* zmap, const char* key, NCjson** jsonp);
EXTERNL int NCZ_subobjects(NCZMAP* map, const char* prefix, const char* tag, char dimsep, NClist* objlist);
EXTERNL int NCZ_grpname_full(int gid, char** pathp);
EXTERNL int ncz_get_var_meta(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var);
EXTERNL int NCZ_comma_parse(const char* s, NClist* list);
EXTERNL int NCZ_swapatomicdata(size_t datalen, void* data, int typesize);
EXTERNL char** NCZ_clonestringvec(size_t len, const char** vec);
EXTERNL void NCZ_freestringvec(size_t len, char** vec);
EXTERNL int NCZ_ischunkname(const char* name,char dimsep);
EXTERNL char* NCZ_chunkpath(struct ChunkKey key);
EXTERNL int NCZ_reclaim_fill_value(NC_VAR_INFO_T* var);
EXTERNL int NCZ_copy_fill_value(NC_VAR_INFO_T* var, void** dstp);
EXTERNL int NCZ_get_maxstrlen(NC_OBJ* obj);
EXTERNL int NCZ_fixed2char(const void* fixed, char** charp, size_t count, int maxstrlen);
EXTERNL int NCZ_char2fixed(const char** charp, void* fixed, size_t count, int maxstrlen);
EXTERNL int NCZ_copy_data(NC_FILE_INFO_T* file, NC_VAR_INFO_T* var, const void* memory, size_t count, int reading, void* copy);
EXTERNL int NCZ_iscomplexjson(const NCjson* value, nc_type typehint);

/* zwalk.c */
EXTERNL int NCZ_read_chunk(int ncid, int varid, size64_t* zindices, void* chunkdata);

#endif /*ZARR_H*/
