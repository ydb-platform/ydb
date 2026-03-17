/* Copyright 2026 University Corporation for Atmospheric
   Research/Unidata. */

#ifndef NCUDFPLUGINS_H
#define NCUDFPLUGINS_H

/**
 * @file
 * @internal This header file contains prototypes and declarations for
 * UDF plugin loading from RC files.
 *
 * @author Ed Hartnett
 */

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Load and initialize UDF plugins from RC file configuration.
 * 
 * This function reads RC file entries for NETCDF.UDF0.LIBRARY through
 * NETCDF.UDF9.LIBRARY and their corresponding INIT and MAGIC keys,
 * then dynamically loads the plugin libraries and calls their
 * initialization functions.
 * 
 * @return NC_NOERR on success (even if no plugins are configured or
 *         some plugins fail to load), error code on critical failure.
 *
 * @author Edward Hartnett
 * @date 2/2/26
 */
EXTERNL int NC_udf_load_plugins(void);

#ifdef __cplusplus
}
#endif

#endif /* NCUDFPLUGINS_H */
