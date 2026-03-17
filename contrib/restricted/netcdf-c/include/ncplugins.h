/*
Copyright (c) 1998-2018 University Corporation for Atmospheric Research/Unidata
See COPYRIGHT for license information.
*/

/*
Common functionality for plugin paths/
For internal use only.
*/

#ifndef NCPLUGINS_H
#define NCPLUGINS_H

/* Opaque */
struct NCPluginList;

#if defined(__cplusplus)
extern "C" {
#endif

EXTERNL int NCZ_plugin_path_initialize(void);
EXTERNL int NCZ_plugin_path_finalize(void);

EXTERNL int NCZ_plugin_path_ndirs(size_t* ndirsp);
EXTERNL int NCZ_plugin_path_get(struct NCPluginList* dirs);
EXTERNL int NCZ_plugin_path_set(struct NCPluginList* dirs);

EXTERNL int NC4_hdf5_plugin_path_initialize(void);
EXTERNL int NC4_hdf5_plugin_path_finalize(void);

EXTERNL int NC4_hdf5_plugin_path_ndirs(size_t* ndirsp);
EXTERNL int NC4_hdf5_plugin_path_get(struct NCPluginList* dirs);
EXTERNL int NC4_hdf5_plugin_path_set(struct NCPluginList* dirs);

#if defined(__cplusplus)
}
#endif

#endif /*NCPLUGINS_H*/
