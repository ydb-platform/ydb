/* Copyright 2018-2018 University Corporation for Atmospheric
   Research/Unidata. */

/**
 * @file This header file containsfilter related  macros, types, and prototypes for
 * the filter code in libnczarr. This header should not be included in
 * code outside libnczarr.
 *
 * @author Dennis Heimbigner
 */

#ifndef ZFILTER_H
#define ZFILTER_H

/* zfilter.c */
/* Dispatch functions are also in zfilter.c */
/* Filterlist management */

/*Mnemonic*/
#define ENCODING 1

/* Opaque */
struct NCZ_Filter;

int NCZ_filter_initialize(void);
int NCZ_filter_finalize(void);
int NCZ_addfilter(NC_FILE_INFO_T*, NC_VAR_INFO_T* var, unsigned int id, size_t nparams, const unsigned int* params);
int NCZ_filter_setup(NC_VAR_INFO_T* var);
int NCZ_filter_freelists(NC_VAR_INFO_T* var);
int NCZ_codec_freelist(NCZ_VAR_INFO_T* zvar);
int NCZ_applyfilterchain(const NC_FILE_INFO_T*, NC_VAR_INFO_T*, NClist* chain, size_t insize, void* indata, size_t* outlen, void** outdata, int encode);
int NCZ_filter_jsonize(const NC_FILE_INFO_T*, const NC_VAR_INFO_T*, struct NCZ_Filter* filter, struct NCjson**);
int NCZ_filter_build(const NC_FILE_INFO_T*, NC_VAR_INFO_T* var, const NCjson* jfilter, int chainindex);
int NCZ_codec_attr(const NC_VAR_INFO_T* var, size_t* lenp, void* data);

#endif /*ZFILTER_H*/
