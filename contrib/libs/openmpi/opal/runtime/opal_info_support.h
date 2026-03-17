/*
 * Copyright (c) 2012-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
* Copyright (c) 2014 Cisco Systems, Inc.  All rights reserved.
* Copyright (c) 2017 IBM Corporation.  All rights reserved.
* $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/** @file **/

#ifndef OPAL_INFO_REGISTER_H
#define OPAL_INFO_REGISTER_H

#include "opal_config.h"

#include "opal/class/opal_list.h"
#include "opal/class/opal_pointer_array.h"
#include "opal/util/cmd_line.h"
#include "opal/mca/base/base.h"

BEGIN_C_DECLS

OPAL_DECLSPEC extern const char *opal_info_path_prefix;

OPAL_DECLSPEC extern const char *opal_info_type_all;
OPAL_DECLSPEC extern const char *opal_info_type_opal;
OPAL_DECLSPEC extern const char *opal_info_component_all;
extern const char *opal_info_param_all;

OPAL_DECLSPEC extern const char *opal_info_ver_full;
extern const char *opal_info_ver_major;
extern const char *opal_info_ver_minor;
extern const char *opal_info_ver_release;
extern const char *opal_info_ver_greek;
extern const char *opal_info_ver_repo;

OPAL_DECLSPEC extern const char *opal_info_ver_all;
extern const char *opal_info_ver_mca;
extern const char *opal_info_ver_type;
extern const char *opal_info_ver_component;


/*
 * Component-related functions
 */
typedef struct {
    opal_list_item_t super;
    char *type;
    opal_list_t *components;
    opal_list_t *failed_components;
} opal_info_component_map_t;
OPAL_DECLSPEC OBJ_CLASS_DECLARATION(opal_info_component_map_t);


OPAL_DECLSPEC int opal_info_init(int argc, char **argv,
                                 opal_cmd_line_t *opal_info_cmd_line);

OPAL_DECLSPEC void opal_info_finalize(void);

OPAL_DECLSPEC void opal_info_register_types(opal_pointer_array_t *mca_types);

OPAL_DECLSPEC int opal_info_register_framework_params(opal_pointer_array_t *component_map);

OPAL_DECLSPEC void opal_info_close_components(void);
OPAL_DECLSPEC void opal_info_err_params(opal_pointer_array_t *component_map);

OPAL_DECLSPEC void opal_info_do_params(bool want_all_in, bool want_internal,
                                       opal_pointer_array_t *mca_type,
                                       opal_pointer_array_t *component_map,
                                       opal_cmd_line_t *opal_info_cmd_line);

OPAL_DECLSPEC void opal_info_show_path(const char *type, const char *value);

OPAL_DECLSPEC void opal_info_do_path(bool want_all, opal_cmd_line_t *cmd_line);

OPAL_DECLSPEC void opal_info_show_mca_params(const char *type,
                                             const char *component,
                                             mca_base_var_info_lvl_t max_level,
                                             bool want_internal);

OPAL_DECLSPEC void opal_info_show_mca_version(const mca_base_component_t *component,
                                              const char *scope, const char *ver_type);

OPAL_DECLSPEC void opal_info_show_component_version(opal_pointer_array_t *mca_types,
                                                    opal_pointer_array_t *component_map,
                                                    const char *type_name,
                                                    const char *component_name,
                                                    const char *scope, const char *ver_type);

OPAL_DECLSPEC char *opal_info_make_version_str(const char *scope,
                                               int major, int minor, int release,
                                               const char *greek,
                                               const char *repo);

OPAL_DECLSPEC void opal_info_show_opal_version(const char *scope);

OPAL_DECLSPEC void opal_info_do_arch(void);

OPAL_DECLSPEC void opal_info_do_hostname(void);

OPAL_DECLSPEC void opal_info_do_type(opal_cmd_line_t *opal_info_cmd_line);

OPAL_DECLSPEC void opal_info_out(const char *pretty_message, const char *plain_message, const char *value);

OPAL_DECLSPEC void opal_info_out_int(const char *pretty_message,
                                     const char *plain_message,
                                     int value);

OPAL_DECLSPEC int opal_info_register_project_frameworks (const char *project_name,
                                                         mca_base_framework_t **frameworks,
                                                         opal_pointer_array_t *component_map);

END_C_DECLS

#endif
