/*********************************************************************
  *   Copyright 2018, UCAR/Unidata
  *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
  *********************************************************************/

#ifndef NCCOMMON_H
#define NCCOMMON_H 1

#include "dapincludes.h"

/* Mnemonics */
#ifndef TRUE
#define TRUE 1
#define FALSE 0
#endif

/* Define an alias for int to indicate an error return */
typedef int NCerror;

#ifndef nullfree
#define nullfree(m) {if((m)!=NULL) {free(m);} else {}}
#endif

/* Misc. code controls */
#define FILLCONSTRAINT TRUE

/* Hopefully, this flag will someday not  be needed */
#define COLUMBIA_HACK "columbia.edu"

/* Use an extended version of the netCDF-4 type system */
#define NC_URL		50
#define NC_SET		51
/* Merge relevant translations of OC types */
#define NC_Dataset	52
#define NC_Sequence	53
#define NC_Structure	54
#define NC_Grid		55
#define NC_Dimension	56
#define NC_Atomic	57

#undef OCCOMPILEBYDEFAULT

#define DEFAULTSTRINGLENGTH 64
/* The sequence limit default is zero because
   most servers do not implement projections
   on sequences.
*/
#define DEFAULTSEQLIMIT 0

/**************************************************/
/* sigh, do the forwards */
struct NCDAPCOMMON;
struct NCprojection;
struct NCselection;
struct Getvara;
struct NCcachenode;
struct NCcache;
struct NCslice;
struct NCsegment;
struct OClist;
struct NCTMODEL {
    int translation;
    char* model;
    unsigned int flags;
};

/* Define the cache control flags */


/* Detail information about each cache item */
typedef struct NCcachenode {
    int wholevariable; /* does this cache node only have wholevariables? */
    int isprefetch; 
    off_t xdrsize;
    DCEconstraint* constraint; /* as used to create this node */
    NClist* vars; /* vars potentially covered by this cache node */
    struct CDFnode* datadds;
    OCddsnode ocroot;
    OCdatanode content;
} NCcachenode;


/* All cache info */
typedef struct NCcache {
    size_t cachelimit; /* max total size for all cached entries */
    size_t cachesize; /* current size */
    size_t cachecount; /* max # nodes in cache */
    NCcachenode* prefetch;
    NClist* nodes; /* cache nodes other than prefetch */
} NCcache;

/**************************************************/
/* The DAP packet info from OC */
typedef struct NCOC {
    OClink conn;
    char* rawurltext; /* as given to nc3d_open */
    char* urltext;    /* as modified by nc3d_open */
    NCURI* url;      /* parse of rawuritext */
    OCdasnode ocdasroot;
    DCEconstraint* dapconstraint; /* from url */
    int inmemory; /* store fetched data in memory? */
} NCOC;

typedef struct NCCDF {
    struct CDFnode* ddsroot; /* constrained dds */
    struct CDFnode* fullddsroot; /* unconstrained dds */
    NClist*  projectedvars; /* vars appearing in nc_open url projections */
    unsigned int defaultstringlength;
    unsigned int defaultsequencelimit; /* global sequence limit;0=>no limit */
    struct NCcache* cache;
    size_t fetchlimit;
    size_t smallsizelimit; /* what constitutes a small object? */
    size_t totalestimatedsize;
    const char* separator; /* constant; do not free */
    /* Following fields should be set from the unconstrained dds only */
    /* global string dimension */
    struct CDFnode* globalstringdim;
    char* recorddimname; /* From DODS_EXTRA */
    struct CDFnode* recorddim;
} NCCDF;

/* Define a structure holding common info for NCDAP */

typedef struct NCDAPCOMMON {
    NC*   controller; /* Parent instance of NCDAPCOMMON */
    NCCDF cdf;
    NCOC oc;
    NCCONTROLS controls; /* Control flags and parameters */
    struct {
	int realfile; /* 1 => we created actual temp file */
	char* filename; /* of the substrate file */
        int nc3id; /* substrate nc4 file ncid used to hold metadata; not same as external id  */
    } substrate;
} NCDAPCOMMON;

/**************************************************/
/* Create our own node tree to mimic ocnode trees*/

/* Each root CDFnode contains info about the whole tree */
typedef struct CDFtree {
    OCddsnode ocroot;
    OCdxd occlass;
    NClist* nodes; /* all nodes in tree*/
    struct CDFnode* root; /* cross link */
    struct NCDAPCOMMON*          owner;
    /* Collected sets of useful nodes */
    NClist*  varnodes; /* nodes which can represent netcdf variables */
    NClist*  seqnodes; /* sequence nodes; */
    NClist*  gridnodes; /* grid nodes */
    NClist*  dimnodes; /* (base) dimension nodes */
    /* Classification flags */
    int restructed; /* Was this tree passed thru restruct? */
} CDFtree;

/* Track the kinds of dimensions */
typedef int CDFdimflags;
#define CDFDIMNORMAL	0x0
#define CDFDIMSEQ	0x1
#define CDFDIMSTRING	0x2
#define CDFDIMCLONE	0x4
#define CDFDIMRECORD	0x20

#define DIMFLAG(d,flag) ((d)->dim.dimflags & (flag))
#define DIMFLAGSET(d,flag) ((d)->dim.dimflags |= (flag))
#define DIMFLAGCLR(d,flag) ((d)->dim.dimflags &= ~(flag))

typedef struct CDFdim {
    CDFdimflags    dimflags;
    struct CDFnode* basedim; /* for duplicate dimensions*/
    struct CDFnode* array; /* parent array node */
    size_t declsize;	    /* from constrained DDS*/
    size_t declsize0;	    /* from unconstrained DDS*/
    int    index1;          /* dimension name index +1; 0=>no index */
} CDFdim;

typedef struct CDFarray {
    NClist*  dimsetall;   /* dimsetplus + inherited */
    NClist*  dimsettrans; /* dimset0 plus inherited(transitive closure) */
    NClist*  dimsetplus;  /* dimset0 + pseudo */
    NClist*  dimset0;     /* original dims from the dds */
    struct CDFnode* stringdim;
    /* Track sequence related information */
    struct CDFnode* seqdim; /* if this node is a sequence */
    /* note: unlike string dim; seqdim is also stored in dimensions vector */
    struct CDFnode* sequence; /* containing usable sequence, if any */
    struct CDFnode* basevar; /* for duplicate grid variables*/
} CDFarray;

typedef struct NCattribute {
    char*   name;
    nc_type etype; /* dap type of the attribute */
    NClist* values; /* strings come from the oc values */
    int     invisible; /* Do not materialize to the user */
} NCattribute;

/* Extend as additional DODS attribute values are defined */
typedef struct NCDODS {
    size_t maxstrlen;
    char* dimname;
} NCDODS;

typedef struct NCD2alignment {
    unsigned long    size; /* size of single instance of this type*/
    unsigned long    alignment; /* alignment of this field */
    unsigned long    offset;    /* offset of this field in parent */
} NCD2alignment;

typedef struct NCtypesize {
    int             aligned; /*  have instance and field been defined? */
    NCD2alignment      instance; /* Alignment, etc for instance data */
    NCD2alignment      field; /* Alignment, etc WRT to parent */
} NCtypesize;

/* Closely mimics struct OCnode*/
typedef struct CDFnode {
    nc_type          nctype;     /* e.g. var, dimension  */
    nc_type          etype;      /* e.g. NC_INT, NC_FLOAT if applicable,*/
    char*            ocname;     /* oc base name */
    char*            ncbasename; /* generally cdflegalname(ocname) */
    char*            ncfullname; /* complete path name from root to this node*/
    OCddsnode        ocnode;     /* oc mirror node*/
    struct CDFnode*  group;	 /* null => in root group */
    struct CDFnode*  container;  /* e.g. struct or sequence, but not group */
    struct CDFnode*  root;
    CDFtree*         tree;       /* root level metadata;only defined if root*/
    CDFdim           dim;        /* nctype == dimension */
    CDFarray         array;      /* nctype == grid,var,etc. with dimensions */
    NClist*          subnodes;      /* if nctype == grid, sequence, etc. */
    NClist*          attributes;    /*NClist<NCattribute*>*/
    NCDODS           dodsspecial;   /* special attributes like maxStrlen */
    nc_type          externaltype;  /* the type as represented to nc_inq*/
    int              ncid;          /* relevant NC id for this object*/
    unsigned long    maxstringlength;
    unsigned long    sequencelimit; /* 0=>unlimited */
    int	     usesequence;   /* If this sequence is usable */
    int             elided;        /* 1 => node does not partipate in naming*/
    struct CDFnode*  basenode;      /* derived tree map to pattern tree */
    int	     invisible;     /* 1 => do not show to user */
    int	     zerodim;       /* 1 => node has a zero dimension */
    /* These two flags track the effects on grids of constraints */
    int             nc_virtual;       /* node added by regrid */
    struct CDFnode* attachment;     /* DDS<->DATADDS cross link*/
    struct CDFnode* pattern;       /* temporary field for regridding */
    /* Fields for use by libncdap4 */
    NCtypesize       typesize;
    int              typeid;        /* when treating field as type */
    int              basetypeid;    /* when typeid is vlen */
    char*            typename;
    char*            vlenname;      /* for sequence types */
    int              singleton;     /* for singleton sequences */
    unsigned long    estimatedsize; /* > 0 Only for var nodes */
    int             whole;         /* projected as whole node */
    int             prefetchable;  /* eligible to be prefetched (if whole) */
} CDFnode;

/**************************************************/
/* Shared procedures */

/* From error.c*/
extern NCerror ocerrtoncerr(OCerror);

extern NCerror attach(CDFnode* xroot, CDFnode* ddstarget);
extern void unattach(CDFnode*);
extern int nodematch(CDFnode* node1, CDFnode* node2);
extern int simplenodematch(CDFnode* node1, CDFnode* node2);
extern CDFnode* findxnode(CDFnode* target, CDFnode* xroot);
extern int constrainable(NCURI*);
extern char* makeconstraintstring(DCEconstraint*);
extern size_t estimatedataddssize(CDFnode* datadds);
extern void canonicalprojection(NClist*, NClist*);
extern NClist* getalldims(NCDAPCOMMON* nccomm, int visibleonly);

/* From cdf.c */
extern NCerror fixgrid(NCDAPCOMMON* drno, CDFnode* grid);
extern NCerror computecdfinfo(NCDAPCOMMON*, NClist*);
extern char* cdfname(char* basename);
extern NCerror augmentddstree(NCDAPCOMMON*, NClist*);
extern NCerror computecdfdimnames(NCDAPCOMMON*);
extern NCerror buildcdftree(NCDAPCOMMON*, OCddsnode, OCdxd, CDFnode**);
extern CDFnode* makecdfnode(NCDAPCOMMON*, char* nm, OCtype,
			    /*optional*/ OCddsnode ocnode, CDFnode* container);
extern void freecdfroot(CDFnode*);

extern NCerror dimimprint(NCDAPCOMMON*);
extern NCerror definedimsets(NCDAPCOMMON*,CDFtree*);
extern NCerror definedimsettrans(NCDAPCOMMON*,CDFtree*);

/* From cache.c */
extern int iscached(NCDAPCOMMON*, CDFnode* target, NCcachenode** cachenodep);
extern NCerror prefetchdata(NCDAPCOMMON*);
extern NCerror markprefetch(NCDAPCOMMON*);
extern NCerror buildcachenode(NCDAPCOMMON*,
	        DCEconstraint* constraint,
		NClist* varlist,
		NCcachenode** cachep,
		NCFLAGS flags);
extern NCcachenode* createnccachenode(void);
extern void freenccachenode(NCDAPCOMMON*, NCcachenode* node);
extern NCcache* createnccache(void);
extern void freenccache(NCDAPCOMMON*, NCcache* cache);

/* Add an extra function whose sole purpose is to allow
   configure(.ac) to test for the presence of thiscode.
*/
extern int nc__opendap(void);

/* Define accessors for the dispatchdata */
#define NCD2_DATA(nc) ((NCDAPCOMMON*)(nc)->dispatchdata)
#define NCD2_DATA_SET(nc,data) ((nc)->dispatchdata = (void*)(data))

#define getncid(drno) (((NC*)drno)->ext_ncid)
#define getdap(drno) ((NCDAPCOMMON*)((NC*)drno)->dispatchdata)
#define getnc3id(drno) (getdap(drno)->substrate.nc3id)

#endif /*NCCOMMON_H*/
