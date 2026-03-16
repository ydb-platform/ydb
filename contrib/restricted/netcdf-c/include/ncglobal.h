/*
Copyright (c) 1998-2018 University Corporation for Atmospheric Research/Unidata
See COPYRIGHT for license information.
*/

/*
Global State and related functions.
*/

#ifndef NCGLOBAL_H
#define NCGLOBAL_H

/* Opaque */
struct NClist;
struct NCURI;
struct NCRCinfo;
struct NCZ_Plugin;
struct GlobalAWS;

/**************************************************/
/* Begin to collect global state info in one place (more to do) */

typedef struct NCglobalstate {
    int initialized;
    char* tempdir; /* track a usable temp dir */
    char* home; /* track $HOME */
    char* cwd; /* track getcwd */
    struct NCRCinfo* rcinfo; /* Currently only one rc file per session */
    struct NClist* pluginpaths; /* Global Plugin State */
    struct GlobalZarr { /* Zarr specific parameters */
	char dimension_separator;
	int default_zarrformat;
	struct NClist* pluginpaths; /* NCZarr mirror of plugin paths */
	struct NClist* codec_defaults;
	struct NClist* default_libs;
	/* All possible HDF5 filter plugins */
	/* Consider onverting to linked list or hash table or
	   equivalent since very sparse */
	struct NCZ_Plugin** loaded_plugins; /*[H5Z_FILTER_MAX+1]*/
	size_t loaded_plugins_max; /* plugin filter id index. 0<loaded_plugins_max<=H5Z_FILTER_MAX */
    } zarr;
    struct GlobalAWS { /* AWS S3 specific parameters/defaults */
	char* default_region;
	char* config_file;
	char* profile;
	char* access_key_id;
	char* secret_access_key;
    } aws;
    struct Alignment { /* H5Pset_alignment parameters */
        int defined; /* 1 => threshold and alignment explicitly set */
	int threshold;
	int alignment;
    } alignment;
    struct ChunkCache {
        size_t size;     /**< Size in bytes of the var chunk cache. */
        size_t nelems;   /**< Number of slots in var chunk cache. */
        float preemption; /**< Chunk cache preemtion policy. */
    } chunkcache;
} NCglobalstate;

/* Externally visible */
typedef struct NCAWSPARAMS NCAWSPARAMS;

#if defined(__cplusplus)
extern "C" {
#endif

/* Implemented in dglobal.c */
struct NCglobalstate* NC_getglobalstate(void);
void NC_freeglobalstate(void);

void NC_clearawsparams(struct GlobalAWS*);

#if defined(__cplusplus)
}
#endif

#endif /*NCGLOBAL_H*/
