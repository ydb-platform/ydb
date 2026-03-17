/*
 * Copyright (c) 2004-2009 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2010 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef ORTE_INFO_TOOL_H
#define ORTE_INFO_TOOL_H
#include "orte_config.h"

#include "opal/class/opal_list.h"
#include "opal/class/opal_pointer_array.h"
#include "opal/util/cmd_line.h"
#include "opal/mca/mca.h"

BEGIN_C_DECLS

/*
 * Globals
 */

extern bool orte_info_pretty;
extern opal_cmd_line_t *orte_info_cmd_line;

extern const char *orte_info_type_all;
extern const char *orte_info_type_opal;
extern const char *orte_info_type_base;

extern opal_pointer_array_t mca_types;


/*
 * Version-related strings and functions
 */

extern const char *orte_info_ver_full;
extern const char *orte_info_ver_major;
extern const char *orte_info_ver_minor;
extern const char *orte_info_ver_release;
extern const char *orte_info_ver_greek;
extern const char *orte_info_ver_svn;

void orte_info_do_version(bool want_all, opal_cmd_line_t *cmd_line);
void orte_info_show_orte_version(const char *scope);
void orte_info_show_component_version(const char *type_name,
                                      const char *component_name,
                                      const char *scope,
                                      const char *ver_type);

/*
 * Parameter/configuration-related functions
 */

extern const char *orte_info_component_all;
extern const char *orte_info_param_all;

extern const char *orte_info_path_prefix;
extern const char *orte_info_path_bindir;
extern const char *orte_info_path_libdir;
extern const char *orte_info_path_incdir;
extern const char *orte_info_path_mandir;
extern const char *orte_info_path_pkglibdir;
extern const char *orte_info_path_sysconfdir;
extern const char *orte_info_path_exec_prefix;
extern const char *orte_info_path_sbindir;
extern const char *orte_info_path_libexecdir;
extern const char *orte_info_path_datarootdir;
extern const char *orte_info_path_datadir;
extern const char *orte_info_path_sharedstatedir;
extern const char *orte_info_path_localstatedir;
extern const char *orte_info_path_infodir;
extern const char *orte_info_path_pkgdatadir;
extern const char *orte_info_path_pkgincludedir;

void orte_info_do_params(bool want_all, bool want_internal);
void orte_info_show_mca_params(const char *type, const char *component,
                               bool want_internal);

void orte_info_do_path(bool want_all, opal_cmd_line_t *cmd_line);
void orte_info_show_path(const char *type, const char *value);

void orte_info_do_arch(void);
void orte_info_do_hostname(void);
void orte_info_do_config(bool want_all);

/*
 * Output-related functions
 */
void orte_info_out(const char *pretty_message,
                   const char *plain_message,
                   const char *value);
void orte_info_out_int(const char *pretty_message,
                       const char *plain_message,
                       int value);
/*
 * Component-related functions
 */
typedef struct {
    opal_list_item_t super;
    char *type;
    opal_list_t *components;
} orte_info_component_map_t;
ORTE_DECLSPEC OBJ_CLASS_DECLARATION(orte_info_component_map_t);

extern opal_pointer_array_t orte_component_map;

void orte_info_components_open(void);
void orte_info_components_close(void);

END_C_DECLS

#endif /* ORTE_INFO_H */
