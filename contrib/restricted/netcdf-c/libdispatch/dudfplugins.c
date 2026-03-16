/* Copyright 2026, UCAR/Unidata.
   See the COPYRIGHT file for more information. */

/**
 * @file
 * @internal This file contains functions for loading UDF plugins from RC files.
 *
 * @author Ed Hartnett
 * @date 2/2/26
 */

#include "config.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "netcdf.h"
#include "netcdf_dispatch.h"
#include "nclog.h"
#include "ncrc.h"
#include "ncudfplugins.h"

/* Platform-specific dynamic loading headers */
#ifdef _WIN32
#include <windows.h>
#else
#include <dlfcn.h>
#endif

/**
 * Load a dynamic library (platform-specific).
 *
 * @param path Full path to the library file.
 * @return Handle to the loaded library, or NULL on failure.
 *
 * @author Edward Hartnett
 * @date 2/2/26
 */
static void*
load_library(const char* path)
{
    void* handle = NULL;
    
#ifdef _WIN32
    handle = (void*)LoadLibraryA(path);
    if (!handle) {
        DWORD err = GetLastError();
        nclog(NCLOGERR, "LoadLibrary failed for %s: error %lu", path, err);
    }
#else
    handle = dlopen(path, RTLD_NOW | RTLD_LOCAL);
    if (!handle) {
        nclog(NCLOGERR, "dlopen failed for %s: %s", path, dlerror());
    }
#endif
    
    return handle;
}

/**
 * Get a symbol from a loaded library (platform-specific).
 *
 * @param handle Handle to the loaded library.
 * @param symbol Name of the symbol to retrieve.
 * @return Pointer to the symbol, or NULL on failure.
 *
 * @author Edward Hartnett
 * @date 2/2/26
 */
static void*
get_symbol(void* handle, const char* symbol)
{
    void* sym = NULL;
    
#ifdef _WIN32
    sym = (void*)GetProcAddress((HMODULE)handle, symbol);
    if (!sym) {
        DWORD err = GetLastError();
        nclog(NCLOGERR, "GetProcAddress failed for %s: error %lu", symbol, err);
    }
#else
    sym = dlsym(handle, symbol);
    if (!sym) {
        nclog(NCLOGERR, "dlsym failed for %s: %s", symbol, dlerror());
    }
#endif
    
    return sym;
}

/**
 * Load a single UDF plugin library and call its initialization function.
 *
 * @param udf_number UDF slot number (0-9).
 * @param library_path Full path to the plugin library.
 * @param init_func Name of the initialization function.
 * @param magic Optional magic number string (can be NULL).
 * @return NC_NOERR on success, error code on failure.
 *
 * @author Edward Hartnett
 * @date 2/2/26
 */
static int
load_udf_plugin(int udf_number, const char* library_path,
                const char* init_func, const char* magic)
{
    int stat = NC_NOERR;
    void* handle = NULL;
    int mode_flag;
#ifndef HAVE_NETCDF_UDF_SELF_REGISTRATION
    NC_Dispatch* dispatch_table = NULL;
    char magic_check[NC_MAX_MAGIC_NUMBER_LEN + 1];
#endif
    
    /* Determine mode flag from UDF number */
    if (udf_number == 0)
        mode_flag = NC_UDF0;
    else if (udf_number == 1)
        mode_flag = NC_UDF1;
    else
        mode_flag = NC_UDF2 << (udf_number - 2);
    
    /* Load the library */
    handle = load_library(library_path);
    if (!handle) {
        stat = NC_ENOTNC;
        goto done;
    }
    
#ifdef HAVE_NETCDF_UDF_SELF_REGISTRATION
    /* Self-registration mode: init function returns NC_Dispatch* */
    {
        NC_Dispatch* (*init_function)(void) = NULL;
        NC_Dispatch* table = NULL;
        
        /* Get the initialization function */
        init_function = (NC_Dispatch* (*)(void))get_symbol(handle, init_func);
        if (!init_function) {
            stat = NC_ENOTNC;
            goto done;
        }
        
        /* Call the initialization function to get the dispatch table */
        table = init_function();
        if (!table) {
            nclog(NCLOGERR, "Plugin init function %s returned NULL", init_func);
            stat = NC_ENOTNC;
            goto done;
        }
        
        /* Verify dispatch ABI version */
        if (table->dispatch_version != NC_DISPATCH_VERSION) {
            nclog(NCLOGERR, "Plugin dispatch ABI mismatch for UDF%d: expected %d, got %d",
                  udf_number, NC_DISPATCH_VERSION, table->dispatch_version);
            stat = NC_EINVAL;
            goto done;
        }
        
        /* Register the dispatch table returned by the plugin */
        if ((stat = nc_def_user_format(mode_flag, table, (char*)magic))) {
            nclog(NCLOGERR, "Failed to register dispatch table for UDF%d: %d",
                  udf_number, stat);
            goto done;
        }
    }
#else
    /* Legacy mode: init function returns int and registers itself */
    {
        int (*init_function)(void) = NULL;
        
        /* Get the initialization function */
        init_function = (int (*)(void))get_symbol(handle, init_func);
        if (!init_function) {
            stat = NC_ENOTNC;
            goto done;
        }
        
        /* Call the initialization function */
        if ((stat = init_function())) {
            nclog(NCLOGERR, "Plugin init function %s failed: %d", init_func, stat);
            goto done;
        }
        
        /* Verify the dispatch table was registered */
        memset(magic_check, 0, sizeof(magic_check));
        if ((stat = nc_inq_user_format(mode_flag, &dispatch_table, magic_check))) {
            nclog(NCLOGERR, "Plugin did not register dispatch table for UDF%d", udf_number);
            goto done;
        }
        
        if (dispatch_table == NULL) {
            nclog(NCLOGERR, "Plugin registered NULL dispatch table for UDF%d", udf_number);
            stat = NC_EINVAL;
            goto done;
        }
        
        /* Verify dispatch ABI version */
        if (dispatch_table->dispatch_version != NC_DISPATCH_VERSION) {
            nclog(NCLOGERR, "Plugin dispatch ABI mismatch for UDF%d: expected %d, got %d",
                  udf_number, NC_DISPATCH_VERSION, dispatch_table->dispatch_version);
            stat = NC_EINVAL;
            goto done;
        }
        
        /* Optionally verify magic number matches */
        if (magic != NULL && strlen(magic_check) > 0) {
            if (strcmp(magic, magic_check) != 0) {
                nclog(NCLOGWARN, "Plugin magic number mismatch for UDF%d: expected %s, got %s",
                      udf_number, magic, magic_check);
            }
        }
    }
#endif
    
    nclog(NCLOGNOTE, "Successfully loaded UDF%d plugin from %s", 
          udf_number, library_path);
    
done:
    /* Handles are intentionally not closed; the OS will reclaim at process exit.
     * The dispatch table and its functions must remain accessible. */
    return stat;
}

/**
 * Load and initialize all UDF plugins from RC file configuration.
 *
 * This function loops through all 10 UDF slots (0-9) and checks for
 * corresponding RC file entries. If both LIBRARY and INIT keys are
 * present for a slot, it attempts to load that plugin.
 *
 * @return NC_NOERR (always succeeds, even if plugins fail to load).
 *
 * @author Edward Hartnett
 * @date 2/2/26
 */
int
NC_udf_load_plugins(void)
{
    int stat = NC_NOERR;
    
    /* Loop through all 10 UDF slots */
    for (int i = 0; i < NC_MAX_UDF_FORMATS; i++) {
        char key_lib[64], key_init[64], key_magic[64];
        const char* lib = NULL;
        const char* init = NULL;
        const char* magic = NULL;
        
        /* Build RC key names for this UDF slot */
        snprintf(key_lib, sizeof(key_lib), "NETCDF.UDF%d.LIBRARY", i);
        snprintf(key_init, sizeof(key_init), "NETCDF.UDF%d.INIT", i);
        snprintf(key_magic, sizeof(key_magic), "NETCDF.UDF%d.MAGIC", i);
        
        /* Look up RC values */
        lib = NC_rclookup(key_lib, NULL, NULL);
        init = NC_rclookup(key_init, NULL, NULL);
        magic = NC_rclookup(key_magic, NULL, NULL);
        
        /* If both LIBRARY and INIT are present, try to load the plugin */
        if (lib && init) {
            if ((stat = load_udf_plugin(i, lib, init, magic))) {
                nclog(NCLOGWARN, "Failed to load UDF%d plugin from %s: %d", i, lib, stat);
            }
        } else if (lib || init) {
            /* Warn about partial configuration */
            nclog(NCLOGWARN, "Ignoring partial UDF%d configuration "
                  "(both NETCDF.UDF%d.LIBRARY and NETCDF.UDF%d.INIT are required)",
                  i, i, i);
        }
    }
    
    /* Always return success - plugin loading failures are not fatal */
    return NC_NOERR;
}
