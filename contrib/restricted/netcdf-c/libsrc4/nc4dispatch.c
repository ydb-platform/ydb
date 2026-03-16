/* Copyright 2005-2018 University Corporation for Atmospheric
   Research/Unidata. */
/**
 * @file
 * @internal This header file contains libsrc4 dispatch
 * initialization, user-defined format, and logging prototypes and
 * macros.
 *
 * @author Dennis Heimbigner, Ed Hartnett
 */

#include "config.h"
#include <stdlib.h>
#include "netcdf.h"
#include "nc4internal.h"
#include "nc4dispatch.h"
#include "nc.h"
#include "ncudfplugins.h"

/* If user-defined formats are in use, we need to declare their
 * dispatch tables. */
#ifdef USE_UDF0
extern NC_Dispatch UDF0_DISPATCH;
#endif /* USE_UDF0 */
#ifdef USE_UDF1
extern NC_Dispatch UDF1_DISPATCH;
#endif /* USE_UDF1 */

extern int nc_plugin_path_initialize(void);
extern int nc_plugin_path_finalize(void);
    
/**
 * @internal Initialize netCDF-4. If user-defined format(s) have been
 * specified in configure, load their dispatch table(s).
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
int
NC4_initialize(void)
{
    int ret = NC_NOERR;

#ifdef USE_UDF0
    /* If user-defined format 0 was specified during configure, set up
     * it's dispatch table. */
    if ((ret = nc_def_user_format(NC_UDF0, UDF0_DISPATCH_FUNC, NULL)))
        return ret;
#endif /* USE_UDF0 */

#ifdef USE_UDF1
    /* If user-defined format 0 was specified during configure, set up
     * it's dispatch table. */
    if ((ret = nc_def_user_format(NC_UDF1F, &UDF1_DISPATCH_FUNC, NULL)))
        return ret;
#endif /* USE_UDF0 */

#ifdef LOGGING
    if(getenv(NCLOGLEVELENV) != NULL) {
        char* slevel = getenv(NCLOGLEVELENV);
        long level = atol(slevel);
#ifdef USE_NETCDF4
        if(level >= 0) {
            nc_set_log_level((int)level);
        }
    }
#endif
#endif

#if defined(USE_HDF5) || defined(NETCDF_ENABLE_NCZARR)
    nc_plugin_path_initialize();
#endif

    /* Load UDF plugins from RC file configuration */
    if ((ret = NC_udf_load_plugins()))
        return ret;

    NC_initialize_reserved();
    return ret;
}

/**
 * @internal Finalize netCDF-4.
 *
 * @return ::NC_NOERR No error.
 * @author Dennis Heimbigner
 */
int
NC4_finalize(void)
{
#if defined(USE_HDF5) || defined(NETCDF_ENABLE_NCZARR)
    nc_plugin_path_finalize();
#endif
    return NC_NOERR;
}
