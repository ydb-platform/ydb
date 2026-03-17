/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

/*
Type declarations and associated constants
are defined here.	
*/

#ifndef D4TYPES_H
#define D4TYPES_H 1

#undef COMPILEBYDEFAULT

#include "ncdap.h"
#include "ncrc.h"
#include "ncauth.h"

/*
Control if struct fields can be map targets.
Currently turned off because semantics are unclear.
*/
#undef ALLOWFIELDMAPS

#define long64 long long
#define ncerror int

/* Misc. code controls */
#define FILLCONSTRAINT TRUE

#define DEFAULTSTRINGLENGTH 64

/* Default Checksum state */
#define DEFAULT_CHECKSUM_STATE 1

/* Size of the checksum */
#define CHECKSUMSIZE 4

/**************************************************/
/* sigh, do the forwards */

typedef struct NCD4INFO NCD4INFO;
typedef enum NCD4CSUM NCD4CSUM;
typedef enum NCD4mode NCD4mode;
typedef enum NCD4format NCD4format;
typedef enum NCD4translation NCD4translation;
typedef struct NCD4curl NCD4curl;
typedef struct NCD4meta NCD4meta;
typedef struct NCD4node NCD4node;
typedef struct NCD4params NCD4params;
typedef struct NCD4HDR NCD4HDR;
typedef struct NCD4offset NCD4offset;
typedef struct NCD4vardata NCD4vardata;
typedef struct NCD4response NCD4response;

/* Define the NCD4HDR flags */
/* Header flags */
#define NCD4_LAST_CHUNK          (1)
#define NCD4_ERR_CHUNK           (2)
#define NCD4_LITTLE_ENDIAN_CHUNK (4)

#define NCD4_ALL_CHUNK_FLAGS (NCD4_LAST_CHUNK|NCD4_ERR_CHUNK|NCD4_LITTLE_ENDIAN_CHUNK)


/**************************************************/
/* DMR Tree node sorts */

/* Concepts from netcdf-4 data model */
/* Define as powers of 2 so we can create a set */
typedef enum NCD4sort {
    NCD4_NULL=0,
    NCD4_ATTR=1,
    NCD4_ATTRSET=2,
    NCD4_XML=4, /* OtherXML */
    NCD4_DIM=8,
    NCD4_GROUP=16, /* Including Dataset */
    NCD4_TYPE=32, /* atom types, opaque, enum, struct or seq */
    NCD4_VAR=64, /* Variable or field */
    NCD4_ECONST=128,
} NCD4sort;

#define ISA(sort,flags) ((sort) & (flags))

#define ISATTR(sort) ISA((sort),(NCD4_ATTR))
#define ISDIM(sort) ISA((sort),(NCD4_DIM))
#define ISGROUP(sort) ISA((sort),(NCD4_GROUP))
#define ISTYPE(sort) ISA((sort),(NCD4_TYPE))
#define ISVAR(sort) ISA((sort),(NCD4_VAR))
#define ISECONST(sort) ISA((sort),(NCD4_ECONST))

/* Define some nc_type aliases */
#define NC_NULL NC_NAT
#define NC_SEQ NC_VLEN
#define NC_STRUCT NC_COMPOUND

#define ISCMPD(subsort) ((subsort) == NC_SEQ || (subsort) == NC_STRUCT)

/**************************************************/
/* Special attributes; When defining netcdf-4,
   these are suppressed, except for UCARTAGMAPS
*/

#define RESERVECHAR '_'

#define UCARTAG         "_edu.ucar."
#define UCARTAGVLEN     "_edu.ucar.isvlen"
#define UCARTAGOPAQUE   "_edu.ucar.opaque.size"
#define UCARTAGORIGTYPE "_edu.ucar.orig.type"
#define UCARTAGUNLIM    "_edu.ucar.isunlimited"

/* These are attributes inserted into the netcdf-4 file */
#define NC4TAGMAPS      "_edu.ucar.maps"

/* dap4.x query keys */
#define DAP4CE		"dap4.ce"
#define DAP4CSUM	"dap4.checksum"

/**************************************************/
/* Misc.*/

/* Define possible translation modes */
enum NCD4translation {
NCD4_NOTRANS = 0, /* Apply straight DAP4->NetCDF4 translation */
NCD4_TRANSNC4 = 1, /* Use _edu.ucar flags to achieve better translation */
};

/* Define possible debug flags */
#define NCF_DEBUG_NONE  0
#define NCF_DEBUG_COPY  1 /* Dump data into the substrate and close it rather than abortiing it */

/* Define possible retrieval modes */
enum NCD4mode {
NCD4_DMR = 1,
NCD4_DAP = 2,
NCD4_DSR = 4
};

/* Define possible retrieval formats */
enum NCD4format {
NCD4_FORMAT_NONE = 0,
NCD4_FORMAT_XML = 1
};

/* Define storage for all the primitive types (plus vlen) */
union ATOMICS {
    char i8[8];
    unsigned char u8[8];
    short i16[4];
    unsigned short u16[4];
    int i32[2];
    unsigned int u32[2];
    long long i64[1];
    unsigned long long u64[1];
    float f32[2];
    double f64[1];
#if SIZEOF_VOIDP == 4
    char* s[2];
#elif SIZEOF_VOIDP == 8
    char* s[1];
#endif
#if (SIZEOF_VOIDP + SIZEOF_SIZE_T) == 8
    nc_vlen_t vl[1];
#endif
};

/**************************************************/
/* Define the structure of the chunk header */

struct NCD4HDR {unsigned int flags; unsigned int count;};

/**************************************************/
/* Define the structure for walking a stream */

/* base <= offset < limit */
struct NCD4offset {
    char* offset; /* use char* so we can do arithmetic */
    char* base;
    char* limit;
};

typedef char* NCD4mark; /* Mark a position */

/**************************************************/
/* !Node type for the NetCDF-4 metadata produced from
   parsing the DMR tree.
   We only use a single node type tagged with the sort.
   Some information is not kept (e.g. attributes).
*/
struct NCD4node {
    NCD4sort sort; /* gross discriminator */
    nc_type subsort; /* subcases of sort */
    char* name; /* Raw unescaped */
    NCD4node*  container; /* parent object: e.g. group, enum, compound...*/
    int visited; /* for recursive walks of all nodes */
    /* Sort specific fields */
    NClist* groups;   /* NClist<NCD4node*> groups in group */
    NClist* vars;   /* NClist<NCD4node*> vars in group, fields in struct/seq */
    NClist* types;   /* NClist<NCD4node*> types in group */
    NClist* dims;    /* NClist<NCD4node*>; dimdefs in group, dimrefs in vars */
    NClist* attributes; /* NClist<NCD4node*> */
    NClist* mapnames;       /* NClist<char*> */
    NClist* maps;       /* NClist<NCD4node*> */
    NClist* xmlattributes; /* NClist<String> */
    NCD4node* basetype;
    struct { /* sort == NCD4_ATTRIBUTE */
        NClist* values;
    } attr;
    struct { /* sort == NCD4_OPAQUE */
        size_t size; /* 0 => var length */
    } opaque;
    struct { /* sort == NCD4_DIMENSION */
	size_t size;
	int isunlimited;
	int isanonymous;
    } dim;
    struct { /* sort == NCD4_ECONST || sort == NCD4_ENUM */    
        union ATOMICS ecvalue;
        NClist* econsts; /* NClist<NCD4node*> */
    } en;
    struct { /* sort == NCD4_GROUP */
	NClist* elements;   /* NClist<NCD4node*> everything at the top level in a group */
        int isdataset;
        char* dapversion;
        char* dmrversion;
        char* datasetname;
        NClist* varbyid; /* NClist<NCD4node*> indexed by varid */
    } group;
    struct { /* Meta Info */
        int id; /* Relevant netcdf id; interpretation depends on sort; */
	int isfixedsize; /* sort == NCD4_TYPE; Is this a fixed size (recursively) type? */
	d4size_t dapsize; /* size of the type as stored in the dap data; will, as a rule,
                             be same as memsize only for types <= NC_UINT64 */
        nc_type cmpdid; /* netcdf id for the compound type created for seq type */
	size_t memsize; /* size of a memory instance without taking dimproduct into account,
                           but taking compound alignment into account  */
        d4size_t offset; /* computed structure field offset in memory */
        size_t alignment; /* computed structure field alignment in memory */
    } meta;
    struct NCD4vardata { /* Data compilation info */
	int valid; /* 1 => this contains valid data */
	D4blob dap4data; /* offset and start pos for this var's data in serialization */
        unsigned remotechecksum; /* toplevel per-variable checksum contained in the data */
        unsigned localchecksum; /* toplevel variable checksum as computed by client */    
	int checksumattr; /* 1 => _DAP4_Checksum_CRC32 is defined */
	unsigned attrchecksum; /* _DAP4_Checksum_CRC32 value; this is the checksum computed by server */
	NCD4response* response; /* Response from which this data is taken */
    } data;
    struct { /* Track netcdf-4 conversion info */
	int isvlen;	/*  _edu.ucar.isvlen */
	/* Split UCARTAGORIGTYPE into group plus name */
	struct {
  	    NCD4node* group;
	    char* name;
	} orig;
	/* Represented elsewhare: _edu.ucar.opaque.size */ 
    } nc4;
};

/* DMR information from a response; this will be passed out of the parse */
struct NCD4meta {
    NCD4INFO* controller;
    int ncid; /* root ncid of the substrate netcdf-4 file;
		 warning: copy of NCD4Info.substrate.nc4id */
    NCD4node* root;
    NClist* allnodes; /*list<NCD4node>*/
    int swap; /* 1 => swap data */
    /* Define some "global" (to a DMR) data */
    NClist* groupbyid; /* NClist<NCD4node*> indexed by groupid >> 16; this is global */
    NCD4node* _bytestring; /* If needed */
    /* Table of known atomictypes */
    NClist* atomictypes; /*list<NCD4node>*/
};

typedef struct NCD4parser {
    NCD4INFO* controller;
    char* input;
    int debuglevel;
    int dapparse; /* 1 => we are parsing the DAP DMR */
    NCD4meta* metadata;
    NCD4response* response;
    /* Capture useful subsets of dataset->allnodes */
    NClist* types; /*list<NCD4node>; user-defined types only*/
    NClist* dims; /*list<NCD4node>*/
    NClist* vars; /*list<NCD4node>*/
    NClist* groups; /*list<NCD4node>*/
    NCD4node* dapopaque; /* Single non-fixed-size opaque type */
} NCD4parser;

/* Capture all the relevant info about the response to a server request */
struct NCD4response { /* possibly processed response from a query */
    NCD4INFO* controller; /* controlling connection */
    D4blob raw; /* complete response in memory */
    int querychecksumming; /* 1 => user specified dap4.ce value */
    int attrchecksumming; /* 1=> _DAP4_Checksum_CRC32 is defined for at least one variable */
    int inferredchecksumming; /* 1 => either query checksum || att checksum */
    int checksumignore; /* 1 => assume checksum, but do not validate */
    int remotelittleendian; /* 1 if the packet says data is little endian */
    NCD4mode  mode; /* Are we reading DMR (only) or DAP (includes DMR) */
    struct NCD4serial {
        size_t dapsize; /* |dap|; this is transient */
        void* dap; /* pointer into raw where dap data starts */ 
        char* dmr;/* copy of dmr */ 
        char* errdata; /* null || error chunk (null terminated) */
        int httpcode; /* returned from last request */
    } serial; /* Dechunked and processed DAP part of the response */
    struct Error { /* Content of any error response */
	char* parseerror;
	int   httpcode;
	char* message;
	char* context;
	char* otherinfo;
    } error;
};

/**************************************************/

/* Curl info */
struct NCD4curl {
    CURL* curl; /* curl handle*/
    NCbytes* packet; 
    struct errdata {/* Hold info for an error return from server */
	char* code;
	char* message;
	long  httpcode;
	char  errorbuf[CURL_ERROR_SIZE]; /* CURLOPT_ERRORBUFFER*/
    } errdata;
    struct {
	int active; /* Activate keepalive? */
	long idle; /* KEEPIDLE value */
	long interval; /* KEEPINTVL value */
    } keepalive; /* keepalive info */
    long buffersize; /* read buffer size */    
};

/**************************************************/
/* Define a structure holding common info */

struct NCD4INFO {
    NC*   controller; /* Parent instance of NCD4INFO */
    char* rawdmrurltext; /* as given to ncd4_open */
    char* dmrurltext;    /* as modified by ncd4_open */
    NCURI* dmruri;      /* parse of rawuritext */
    NCD4curl* curl;
    int inmemory; /* store fetched data in memory? */
    NCD4meta* dmrmetadata; /* Independent of responses */
    NClist* responses; /* NClist<NCD4response> all responses from this curl handle */
    struct { /* Properties that are per-platform */
        int hostlittleendian; /* 1 if the host is little endian */
    } platform;
    struct {
	int realfile; /* 1 => we created actual temp file */
	char* filename; /* of the substrate file */
        int nc4id; /* substrate nc4 file ncid used to hold metadata; not same as external id  */
    } substrate;
    struct {
        NCCONTROLS  flags;
        NCCONTROLS  debugflags;
	NCD4translation translation;
	char substratename[NC_MAX_NAME];
	size_t opaquesize; /* default opaque size */
    } controls;
    NCauth* auth;
    struct {
	char* filename;
    } fileproto;
    int debuglevel;
    NClist* blobs;
};

#endif /*D4TYPES_H*/
