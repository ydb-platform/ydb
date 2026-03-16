/* Copyright 2018-2018 University Corporation for Atmospheric
   Research/Unidata. */

/**
 * @file This header file contains macros, types, and prototypes for
 * the ZARR code in libzarr. This header should not be included in
 * code outside libzarr.
 *
 * @author Dennis Heimbigner, Ed Hartnett
 */

#ifndef ZINTERNAL_H
#define ZINTERNAL_H

#define ZARRVERSION "2"

/* NCZARRVERSION is independent of Zarr version,
   but NCZARRVERSION => ZARRVERSION */
#define NCZARRVERSION "2.0.0"

/* These have to do with creating chunked datasets in ZARR. */
#define NCZ_CHUNKSIZE_FACTOR (10)
#define NCZ_MIN_CHUNK_SIZE (2)

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

#define Z2GROUPROOT "/.zgroup"
#define Z2GROUP ".zgroup"
#define Z2ATTRS ".zattrs"
#define Z2ARRAY ".zarray"

/* V2 Reserved Attributes */
/*
For nczarr version 2.x.x, the following (key,value)
pairs are stored in .zgroup and/or .zarray.

Inserted into /.zattrs in root group
_nczarr_superblock: {"version": "2.0.0"}

Inserted into any group level .zattrs 
"_nczarr_group": "{
\"dimensions\": [{name: <dimname>, size: <integer>, unlimited: 1|0},...],
\"arrays\": [\"v1\", \"v2\", ...]
\"groups\": [\"g1\", \"g2\", ...]
}"

Inserted into any array level .zattrs
"_nczarr_array": "{
\"dimension_references\": [\"/g1/g2/d1\", \"/d2\",...]
\"storage\": \"scalar\"|\"contiguous\"|\"chunked\"
}"

Inserted into any .zattrs
"_nczarr_attr": "{
\"types\": {\"attr1\": \"<i4\", \"attr2\": \"<i1\",...}
}
*/

#define NCZ_V2_SUPERBLOCK "_nczarr_superblock"
#define NCZ_V2_GROUP   "_nczarr_group"
#define NCZ_V2_ARRAY   "_nczarr_array"
#define NCZ_V2_ATTR    "_nczarr_attr" /* Must match value in include/nc4internal.h */

#define NCZARRCONTROL "nczarr"
#define PUREZARRCONTROL "zarr"
#define XARRAYCONTROL "xarray"
#define NOXARRAYCONTROL "noxarray"
#define XARRAYSCALAR "_scalar_"

#define NC_NCZARR_MAXSTRLEN_ATTR "_nczarr_maxstrlen"
#define NC_NCZARR_DEFAULT_MAXSTRLEN_ATTR "_nczarr_default_maxstrlen"

#define LEGAL_DIM_SEPARATORS "./"
#define DFALT_DIM_SEPARATOR '.'

#define islegaldimsep(c) ((c) != '\0' && strchr(LEGAL_DIM_SEPARATORS,(c)) != NULL)

/* Default max string length for fixed length strings */
#define NCZ_MAXSTR_DEFAULT 128

/* Mnemonics */
#define ZCLOSE	 1 /* this is closeorabort as opposed to enddef */
#define ZREADING 1 /* this is reading data rather than writing */

/* Useful macro */
#define ncidforx(file,grpid) ((file)->controller->ext_ncid | (grpid))
#define ncidfor(var) ncidforx((var)->container->nc4_info,(var)->container->hdr.id)

/**************************************************/
/* Forward */

struct NClist;
struct NCjson;
struct NCauth;
struct NCZMAP;
struct NCZChunkCache;

/**************************************************/
/* Define annotation data for NCZ objects */

/* Common fields for all annotations */
typedef struct NCZcommon {
    NC_FILE_INFO_T* file; /* root of the dataset tree */
} NCZcommon;

/** Struct to hold ZARR-specific info for the file. */
typedef struct NCZ_FILE_INFO {
    NCZcommon common;
    struct NCZMAP* map; /* implementation */
    struct NCauth* auth;
    struct NCZ_Metadata metadata;
    struct nczarr {
	int zarr_version;
	struct {
	    unsigned long major;
	    unsigned long minor;
	    unsigned long release;
	} nczarr_version;
    } zarr;
    int creating; /* 1=> created 0=>open */
    int native_endianness; /* NC_ENDIAN_LITTLE | NC_ENDIAN_BIG */
    NClist* controllist; /* Envv format */
    struct Controls {
        size64_t flags;
#		define FLAG_PUREZARR    1
#		define FLAG_SHOWFETCH   2
#		define FLAG_LOGGING     4
#		define FLAG_XARRAYDIMS  8
#		define FLAG_NCZARR_KEY  16 /* _nczarr_xxx keys are stored in object and not in _nczarr_attrs */
#		define FLAG_CONSOLIDATED 32
	NCZM_IMPL mapimpl;
    } controls;
    int default_maxstrlen; /* default max str size for variables of type string */
} NCZ_FILE_INFO_T;

/* This is a struct to handle the dim metadata. */
typedef struct NCZ_DIM_INFO {
    NCZcommon common;
} NCZ_DIM_INFO_T;

/** Struct to hold ZARR-specific info for attributes. */
typedef struct  NCZ_ATT_INFO {
    NCZcommon common;
} NCZ_ATT_INFO_T;

/* Struct to hold ZARR-specific info for a group. */
typedef struct NCZ_GRP_INFO {
    NCZcommon common;
    /* Read .zgroup and .zattrs once */
    struct ZARROBJ {
	char* prefix; /* prefix of .zgroup and .zattrs */
	NCjson* obj; /* .zgroup|.zarray */
	NCjson* atts;
        int nczv1;   /* 1 => _nczarr_xxx are in obj and not attributes */
    } zgroup;
} NCZ_GRP_INFO_T;

/* Struct to hold ZARR-specific info for a variable. */
typedef struct NCZ_VAR_INFO {
    NCZcommon common;
    size64_t chunkproduct; /* product of chunksizes */
    size64_t chunksize; /* chunkproduct * typesize */
    int order; /* 1=>column major, 0=>row major (default); not currently enforced */
    size_t scalar;
    struct NCZChunkCache* cache;
    struct NClist* xarray; /* names from _ARRAY_DIMENSIONS */
    char dimension_separator; /* '.' | '/' */
    NClist* incompletefilters;
    int maxstrlen; /* max length of strings for this variable */
    /* Read .zarray and .zattrs once */
    struct ZARROBJ zarray;
    struct ZARROBJ zattrs;
} NCZ_VAR_INFO_T;

/* Struct to hold ZARR-specific info for a field. */
typedef struct NCZ_FIELD_INFO {
    NCZcommon common;
} NCZ_FIELD_INFO_T;

/* Struct to hold ZARR-specific info for a type. */
typedef struct NCZ_TYPE_INFO {
    NCZcommon common;
} NCZ_TYPE_INFO_T;

#if 0
/* Define the contents of the .nczcontent object */
/* The .nczcontent field stores the following:
   1. List of (name,length) for dims in the group
   2. List of (name,type) for user-defined types in the group
   3. List of var names in the group
   4. List of subgroups names in the group
*/
typedef struct NCZCONTENT{
    NClist* dims;
    NClist* types; /* currently not used */
    NClist* vars;
    NClist* grps;
} NCZCONTENT;
#endif

/**************************************************/

extern int ncz_initialized; /**< True if initialization has happened. */

/* Forward */
struct NCZ_Filterspec;

/* zinternal.c */
int NCZ_initialize(void);
int NCZ_finalize(void);
int NCZ_initialize_internal(void);
int NCZ_finalize_internal(void);
int NCZ_ensure_fill_value(NC_VAR_INFO_T* var);
int ncz_find_grp_var_att(int ncid, int varid, const char *name, int attnum,
                              int use_name, char *norm_name, NC_FILE_INFO_T** file,
                              NC_GRP_INFO_T** grp, NC_VAR_INFO_T** var,
                              NC_ATT_INFO_T** att);
int NCZ_set_log_level(void);

/* zcache.c */
int ncz_adjust_var_cache(NC_GRP_INFO_T* grp, NC_VAR_INFO_T* var);
int NCZ_set_var_chunk_cache(int ncid, int varid, size_t size, size_t nelems, float preemption);

/* zfile.c */
int ncz_enddef_netcdf4_file(NC_FILE_INFO_T*);
int ncz_closeorabort(NC_FILE_INFO_T*, void* params, int abort);

/* zclose.c */
int ncz_close_ncz_file(NC_FILE_INFO_T* file, int abort);
int NCZ_zclose_var1(NC_VAR_INFO_T* var);

/* zattr.c */
int ncz_getattlist(NC_GRP_INFO_T *grp, int varid, NC_VAR_INFO_T **varp, NCindex **attlist);
int ncz_create_fillvalue(NC_VAR_INFO_T* var);
int ncz_makeattr(NC_OBJ*, NCindex* attlist, const char* name, nc_type typid, size_t len, void* values, NC_ATT_INFO_T**);

/* zvar.c */
int ncz_gettype(NC_FILE_INFO_T*, NC_GRP_INFO_T*, int xtype, NC_TYPE_INFO_T** typep);
int ncz_find_default_chunksizes2(NC_GRP_INFO_T *grp, NC_VAR_INFO_T *var);
int NCZ_ensure_quantizer(int ncid, NC_VAR_INFO_T* var);

/* Undefined */
/* Find var, doing lazy var metadata read if needed. */
int ncz_find_grp_file_var(int ncid, int varid, NC_FILE_INFO_T** file,
                             NC_GRP_INFO_T** grp, NC_VAR_INFO_T** var);

#endif /* ZINTERNAL_H */

