/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT file for more information. */
/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Board of Trustees of the University of Illinois.         *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://support.hdfgroup.org/ftp/hdf5/releases.  *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* This include file is used if one wished to build a filter plugin
   independent of HDF5. See examples in the plugins directory
*/

#ifndef NETCDF_FILTER_BUILD_H
#define NETCDF_FILTER_BUILD_H 1

#include "netcdf_filter_hdf5_build.h"

/**************************************************/
/* Build To a NumCodecs-style C-API for Filters */

/* Version of the NCZ_codec_t structure */
#define NCZ_CODEC_CLASS_VER 1

/* List of the kinds of NCZ_codec_t formats */
#define NCZ_CODEC_HDF5 1 /* HDF5 <-> Codec converter */

/* Defined flags for filter invocation (not stored); powers of two */
#define NCZ_FILTER_DECODE 0x00000001

/* External Discovery Functions */

/*
Obtain a pointer to an instance of NCZ_codec_class_t.

NCZ_get_codec_info(void) --  returns pointer to instance of NCZ_codec_class_t.
			      Instance an be recast based on version+sort to the plugin type specific info.
So the void* return value is typically actually of type NCZ_codec_class_t*.

Signature: typedef const void* (*NCZ_get_codec_info_proto)(void);

The current object returned by NCZ_get_codec_info is a
 pointer to an instance of NCZ_codec_t.

The key to this struct are the several function pointers that do
initialize/finalize and conversion between codec JSON and HDF5
parameters.  The function pointers defined in NCZ_codec_t
manipulate HDF5 parameters and NumCodec JSON.

Obtain a pointer to an instance of NCZ_codec_class_t.

NCZ_get_codec_info(void) --  returns pointer to instance of NCZ_codec_class_t.
			      Instance an be recast based on version+sort to the plugin type specific info.
So the void* return value is typically actually of type NCZ_codec_class_t*.
*/
typedef const void* (*NCZ_get_codec_info_proto)(void);

/*
Obtain a pointer to a NULL terminated vector of NCZ_codec_class_t*.

NCZ_codec_info_defaults(void) --  returns pointer to a vector of pointers to instances of NCZ_codec_class_t. The vector is NULL terminated.
So the void* return value is typically actually of type NCZ_codec_class_t**.

Signature: typedef const void* (*NCZ_codec_info_defaults_proto)(void);

This entry point is used to return the codec information for
multiple filters that otherwise do not have codec information defined.
*/
typedef const void* (*NCZ_codec_info_defaults_proto)(void);

/* The current object returned by NCZ_get_plugin_info is a
   pointer to an instance of NCZ_codec_t.

The key to this struct are the several function pointers that do initialize/finalize
and conversion between codec JSON and HDF5 parameters.

The function pointers defined in NCZ_codec_t manipulate HDF5 parameters and NumCodec JSON.

* Initialize use of the filter. This is invoked when a filter is loaded.

void (*NCZ_codec_initialize)(void);

* Finalize use of the filter. Since HDF5 does not provide this functionality, the codec may need to do it.
See H5Zblosc.c for an example. This function is invoked when a filter is unloaded.

void (*NCZ_codec_finalize)(void);

* Convert a JSON representation to an HDF5 representation. Invoked when a NumCodec JSON Codec is extracted
from Zarr metadata.

int (*NCZ_codec_to_hdf5)(const char* codec, int* nparamsp, unsigned** paramsp);

@param codec   -- (in) ptr to JSON string representing the codec.
@param nparamsp -- (out) store the length of the converted HDF5 unsigned vector
@param paramsp -- (out) store a pointer to the converted HDF5 unsigned vector;
                  caller frees. Note the double indirection.
@return -- a netcdf-c error code.


* Convert an HDF5 vector of visible parameters to a JSON representation.

int (*NCZ_hdf5_to_codec)(size_t nparams, const unsigned* params, char** codecp);

@param nparams -- (in) the length of the HDF5 unsigned vector
@param params -- (in) pointer to the HDF5 unsigned vector.
@param codecp -- (out) store the string representation of the codec; caller must free.
@return -- a netcdf-c error code.

* Convert a set of visible parameters to a set of working parameters using extra environmental information.
Also allows for changes to the visible parameters. Invoked before filter is actually used.

int (*NCZ_modify_parameters)(int ncid, int varid, size_t* vnparamsp, unsigned** vparamsp, size_t* wnparamsp, unsigned** wparamsp);

@param ncid -- (in) ncid of the variable's group
@param varid -- (in) varid of the variable
@params vnparamsp -- (in/out) number of visible parameters
@params vparamsp -- (in/out) vector of visible parameters
@params wnparamsp -- (out) number of working parameters
@params wparamsp -- (out) vector of working parameters
@return -- a netcdf-c error code.

* Convert an HDF5 vector of visible parameters to a JSON representation.

int (*NCZ_hdf5_to_codec)(size_t nparams, const unsigned* params, char** codecp);

@param nparams -- (in) the length of the HDF5 unsigned vector
@param params -- (in) pointer to the HDF5 unsigned vector.
@param codecp -- (out) store the string representation of the codec; caller must free.
@return -- a netcdf-c error code.

*/

/*
The struct that provides the necessary filter info.
The combination of version + sort uniquely determines
the format of the remainder of the struct
*/
typedef struct NCZ_codec_t {
    int version; /* Version number of the struct */
    int sort; /* Format of remainder of the struct;
                 Currently always NCZ_CODEC_HDF5 */
    const char* codecid;            /* The name/id of the codec */
    unsigned int hdf5id; /* corresponding hdf5 id */
    void (*NCZ_codec_initialize)(void);
    void (*NCZ_codec_finalize)(void);
    int (*NCZ_codec_to_hdf5)(const char* codec, size_t* nparamsp, unsigned** paramsp);
    int (*NCZ_hdf5_to_codec)(size_t nparams, const unsigned* params, char** codecp);
    int (*NCZ_modify_parameters)(int ncid, int varid, size_t* vnparamsp, unsigned** vparamsp, size_t* wnparamsp, unsigned** wparamsp);
} NCZ_codec_t;

#ifndef NC_UNUSED
#define NC_UNUSED(var) (void)var
#endif

#ifndef DLLEXPORT
#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif
#endif

#endif /*NETCDF_FILTER_BUILD_H*/
