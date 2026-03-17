/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2015 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010-2016 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2011-2012 University of Houston. All rights reserved.
 * Copyright (c) 2016-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2017 IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include <string.h>
#include <ctype.h>

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include <errno.h>

#include "opal/class/opal_list.h"
#include "opal/class/opal_pointer_array.h"

#include "opal/util/output.h"
#include "opal/util/cmd_line.h"
#include "opal/util/error.h"
#include "opal/util/argv.h"
#include "opal/util/show_help.h"
#include "opal/runtime/opal.h"
#include "opal/dss/dss.h"
#include "opal/mca/base/mca_base_pvar.h"

#include "opal/include/opal/frameworks.h"

#include "opal/mca/installdirs/installdirs.h"

#include "opal/runtime/opal_info_support.h"
#include "opal/mca/base/mca_base_component_repository.h"

const char *opal_info_path_prefix = "prefix";
const char *opal_info_path_bindir = "bindir";
const char *opal_info_path_libdir = "libdir";
const char *opal_info_path_incdir = "incdir";
const char *opal_info_path_mandir = "mandir";
const char *opal_info_path_pkglibdir = "pkglibdir";
const char *opal_info_path_sysconfdir = "sysconfdir";
const char *opal_info_path_exec_prefix = "exec_prefix";
const char *opal_info_path_sbindir = "sbindir";
const char *opal_info_path_libexecdir = "libexecdir";
const char *opal_info_path_datarootdir = "datarootdir";
const char *opal_info_path_datadir = "datadir";
const char *opal_info_path_sharedstatedir = "sharedstatedir";
const char *opal_info_path_localstatedir = "localstatedir";
const char *opal_info_path_infodir = "infodir";
const char *opal_info_path_pkgdatadir = "pkgdatadir";
const char *opal_info_path_pkgincludedir = "pkgincludedir";

bool opal_info_pretty = true;
mca_base_register_flag_t opal_info_register_flags = MCA_BASE_REGISTER_ALL;

const char *opal_info_type_all = "all";
const char *opal_info_type_opal = "opal";
const char *opal_info_component_all = "all";
const char *opal_info_param_all = "all";

const char *opal_info_ver_full = "full";
const char *opal_info_ver_major = "major";
const char *opal_info_ver_minor = "minor";
const char *opal_info_ver_release = "release";
const char *opal_info_ver_greek = "greek";
const char *opal_info_ver_repo = "repo";

const char *opal_info_ver_all = "all";
const char *opal_info_ver_mca = "mca";
const char *opal_info_ver_type = "type";
const char *opal_info_ver_component = "component";

static int opal_info_registered = 0;

static void component_map_construct(opal_info_component_map_t *map)
{
    map->type = NULL;
}
static void component_map_destruct(opal_info_component_map_t *map)
{
    if (NULL != map->type) {
        free(map->type);
    }
    /* the type close functions will release the
     * list of components
     */
}
OBJ_CLASS_INSTANCE(opal_info_component_map_t,
                   opal_list_item_t,
                   component_map_construct,
                   component_map_destruct);

static void opal_info_show_failed_component(const mca_base_component_repository_item_t* ri,
                                            const char *error_msg);

int opal_info_init(int argc, char **argv,
                   opal_cmd_line_t *opal_info_cmd_line)
{
    int ret;
    bool want_help = false;
    bool cmd_error = false;
    char **app_env = NULL, **global_env = NULL;

    /* Initialize the argv parsing handle */
    if (OPAL_SUCCESS != (ret = opal_init_util(&argc, &argv))) {
        opal_show_help("help-opal_info.txt", "lib-call-fail", true,
                       "opal_init_util", __FILE__, __LINE__, NULL);
        exit(ret);
    }

    /* add the cmd line options */
    opal_cmd_line_make_opt3(opal_info_cmd_line, 'V', NULL, "version", 0,
                            "Show version of Open MPI");
    opal_cmd_line_make_opt3(opal_info_cmd_line, '\0', NULL, "param", 2,
                            "Show MCA parameters.  The first parameter is the framework (or the keyword \"all\"); the second parameter is the specific component name (or the keyword \"all\").");
    opal_cmd_line_make_opt3(opal_info_cmd_line, '\0', NULL, "params", 2,
                            "Synonym for --param");
    opal_cmd_line_make_opt3(opal_info_cmd_line, '\0', NULL, "internal", 0,
                            "Show internal MCA parameters (not meant to be modified by users)");
    opal_cmd_line_make_opt3(opal_info_cmd_line, '\0', NULL, "path", 1,
                            "Show paths that Open MPI was configured with.  Accepts the following parameters: prefix, bindir, libdir, incdir, mandir, pkglibdir, sysconfdir, all");
    opal_cmd_line_make_opt3(opal_info_cmd_line, '\0', NULL, "arch", 0,
                            "Show architecture Open MPI was compiled on");
    opal_cmd_line_make_opt3(opal_info_cmd_line, 'c', NULL, "config", 0,
                            "Show configuration options");
    opal_cmd_line_make_opt3(opal_info_cmd_line, 't', NULL, "type", 1,
                            "Show internal MCA parameters with the type specified in parameter.");
    opal_cmd_line_make_opt3(opal_info_cmd_line, 'h', NULL, "help", 0,
                            "Show this help message");
    opal_cmd_line_make_opt3(opal_info_cmd_line, '\0', NULL, "pretty-print", 0,
                            "When used in conjunction with other parameters, the output is displayed in 'pretty-print' format (default)");
    opal_cmd_line_make_opt3(opal_info_cmd_line, '\0', NULL, "parsable", 0,
                            "When used in conjunction with other parameters, the output is displayed in a machine-parsable format");
    opal_cmd_line_make_opt3(opal_info_cmd_line, '\0', NULL, "parseable", 0,
                            "Synonym for --parsable");
    opal_cmd_line_make_opt3(opal_info_cmd_line, '\0', NULL, "hostname", 0,
                            "Show the hostname that Open MPI was configured and built on");
    opal_cmd_line_make_opt3(opal_info_cmd_line, 'a', NULL, "all", 0,
                            "Show all configuration options and MCA parameters");
    opal_cmd_line_make_opt3(opal_info_cmd_line, 'l', NULL, "level", 1,
                            "Show only variables with at most this level (1-9)");
    opal_cmd_line_make_opt3(opal_info_cmd_line, 's', NULL, "selected-only", 0,
                            "Show only variables from selected components");
    opal_cmd_line_make_opt3(opal_info_cmd_line, '\0', NULL, "show-failed", 0,
                            "Show the components that failed to load along with the reason why they failed.");

    /* set our threading level */
    opal_set_using_threads(false);

    /* Get MCA parameters, if any */
    if( OPAL_SUCCESS != mca_base_open() ) {
        opal_show_help("help-opal_info.txt", "lib-call-fail", true, "mca_base_open", __FILE__, __LINE__ );
        opal_finalize_util();
        return OPAL_ERROR;
    }
    mca_base_cmd_line_setup(opal_info_cmd_line);

    /* Initialize the opal_output system */
    if (!opal_output_init()) {
        return OPAL_ERROR;
    }

    /* Do the parsing */
    ret = opal_cmd_line_parse(opal_info_cmd_line, false, false, argc, argv);
    if (OPAL_SUCCESS != ret) {
        cmd_error = true;
        if (OPAL_ERR_SILENT != ret) {
            fprintf(stderr, "%s: command line error (%s)\n", argv[0],
                    opal_strerror(ret));
        }
    }
    if (!cmd_error &&
        (opal_cmd_line_is_taken(opal_info_cmd_line, "help") ||
         opal_cmd_line_is_taken(opal_info_cmd_line, "h"))) {
        char *str, *usage;

        want_help = true;
        usage = opal_cmd_line_get_usage_msg(opal_info_cmd_line);
        str = opal_show_help_string("help-opal_info.txt", "usage",
                                    true, usage);
        if (NULL != str) {
            printf("%s", str);
            free(str);
        }
        free(usage);
    }

    /* If we had a cmd line parse error, or we showed the help
       message, it's time to exit. */
    if (cmd_error || want_help) {
        mca_base_close();
        OBJ_RELEASE(opal_info_cmd_line);
        opal_finalize_util();
        exit(cmd_error ? 1 : 0);
    }

    mca_base_cmd_line_process_args(opal_info_cmd_line, &app_env, &global_env);


    /* set the flags */
    if (opal_cmd_line_is_taken(opal_info_cmd_line, "pretty-print")) {
        opal_info_pretty = true;
    } else if (opal_cmd_line_is_taken(opal_info_cmd_line, "parsable") || opal_cmd_line_is_taken(opal_info_cmd_line, "parseable")) {
        opal_info_pretty = false;
    }

    if (opal_cmd_line_is_taken(opal_info_cmd_line, "selected-only")) {
        /* register only selected components */
        opal_info_register_flags = MCA_BASE_REGISTER_DEFAULT;
    }

    if( opal_cmd_line_is_taken(opal_info_cmd_line, "show-failed") ) {
        mca_base_component_track_load_errors = true;
    }

    return OPAL_SUCCESS;
}

void opal_info_finalize(void)
{
    opal_finalize_util();
}

static int info_register_framework (mca_base_framework_t *framework, opal_pointer_array_t *component_map)
{
    opal_info_component_map_t *map;
    int rc;

    rc = mca_base_framework_register(framework, opal_info_register_flags);
    if (OPAL_SUCCESS != rc && OPAL_ERR_BAD_PARAM != rc) {
        return rc;
    }

    if (NULL != component_map) {
        map = OBJ_NEW(opal_info_component_map_t);
        map->type = strdup(framework->framework_name);
        map->components = &framework->framework_components;
        map->failed_components = &framework->framework_failed_components;
        opal_pointer_array_add(component_map, map);
    }

    return rc;
}

int opal_info_register_project_frameworks (const char *project_name, mca_base_framework_t **frameworks,
                                           opal_pointer_array_t *component_map)
{
    int i, rc=OPAL_SUCCESS;

    for (i=0; NULL != frameworks[i]; i++) {
        if (OPAL_SUCCESS != (rc = info_register_framework(frameworks[i], component_map))) {
            if (OPAL_ERR_BAD_PARAM == rc) {
                fprintf(stderr, "\nA \"bad parameter\" error was encountered when opening the %s %s framework\n",
                        project_name, frameworks[i]->framework_name);
                fprintf(stderr, "The output received from that framework includes the following parameters:\n\n");
            } else if (OPAL_ERR_NOT_AVAILABLE != rc) {
                fprintf(stderr, "%s_info_register: %s failed\n", project_name, frameworks[i]->framework_name);
                rc = OPAL_ERROR;
            } else {
                continue;
            }

            break;
        }
    }

    return rc;
}

void opal_info_register_types(opal_pointer_array_t *mca_types)
{
    int i;

    /* add the top-level types */
    opal_pointer_array_add(mca_types, "mca");
    opal_pointer_array_add(mca_types, "opal");

    /* push all the types found by autogen */
    for (i=0; NULL != opal_frameworks[i]; i++) {
        opal_pointer_array_add(mca_types, opal_frameworks[i]->framework_name);
    }
}

int opal_info_register_framework_params(opal_pointer_array_t *component_map)
{
    int rc;

    if (opal_info_registered++) {
        return OPAL_SUCCESS;
    }

    /* Register mca/base parameters */
    if( OPAL_SUCCESS != mca_base_open() ) {
        opal_show_help("help-opal_info.txt", "lib-call-fail", true, "mca_base_open", __FILE__, __LINE__ );
        return OPAL_ERROR;
    }

    /* Register the OPAL layer's MCA parameters */
    if (OPAL_SUCCESS != (rc = opal_register_params())) {
        fprintf(stderr, "opal_info_register: opal_register_params failed\n");
        return rc;
    }

    return opal_info_register_project_frameworks("opal", opal_frameworks, component_map);
}


void opal_info_close_components(void)
{
    int i;

    assert(opal_info_registered);
    if (--opal_info_registered) {
        return;
    }

    for (i=0; NULL != opal_frameworks[i]; i++) {
        (void) mca_base_framework_close(opal_frameworks[i]);
    }

    /* release our reference to MCA */
    mca_base_close ();
}


void opal_info_show_path(const char *type, const char *value)
{
    char *pretty, *path;

    pretty = strdup(type);
    pretty[0] = toupper(pretty[0]);

    asprintf(&path, "path:%s", type);
    opal_info_out(pretty, path, value);
    free(pretty);
    free(path);
}

void opal_info_do_path(bool want_all, opal_cmd_line_t *cmd_line)
{
    int i, count;
    char *scope;

    /* Check bozo case */
    count = opal_cmd_line_get_ninsts(cmd_line, "path");
    for (i = 0; i < count; ++i) {
        scope = opal_cmd_line_get_param(cmd_line, "path", i, 0);
        if (0 == strcmp("all", scope)) {
            want_all = true;
            break;
        }
    }

    if (want_all) {
        opal_info_show_path(opal_info_path_prefix, opal_install_dirs.prefix);
        opal_info_show_path(opal_info_path_exec_prefix, opal_install_dirs.exec_prefix);
        opal_info_show_path(opal_info_path_bindir, opal_install_dirs.bindir);
        opal_info_show_path(opal_info_path_sbindir, opal_install_dirs.sbindir);
        opal_info_show_path(opal_info_path_libdir, opal_install_dirs.libdir);
        opal_info_show_path(opal_info_path_incdir, opal_install_dirs.includedir);
        opal_info_show_path(opal_info_path_mandir, opal_install_dirs.mandir);
        opal_info_show_path(opal_info_path_pkglibdir, opal_install_dirs.opallibdir);
        opal_info_show_path(opal_info_path_libexecdir, opal_install_dirs.libexecdir);
        opal_info_show_path(opal_info_path_datarootdir, opal_install_dirs.datarootdir);
        opal_info_show_path(opal_info_path_datadir, opal_install_dirs.datadir);
        opal_info_show_path(opal_info_path_sysconfdir, opal_install_dirs.sysconfdir);
        opal_info_show_path(opal_info_path_sharedstatedir, opal_install_dirs.sharedstatedir);
        opal_info_show_path(opal_info_path_localstatedir, opal_install_dirs.localstatedir);
        opal_info_show_path(opal_info_path_infodir, opal_install_dirs.infodir);
        opal_info_show_path(opal_info_path_pkgdatadir, opal_install_dirs.opaldatadir);
        opal_info_show_path(opal_info_path_pkglibdir, opal_install_dirs.opallibdir);
        opal_info_show_path(opal_info_path_pkgincludedir, opal_install_dirs.opalincludedir);
    } else {
        count = opal_cmd_line_get_ninsts(cmd_line, "path");
        for (i = 0; i < count; ++i) {
            scope = opal_cmd_line_get_param(cmd_line, "path", i, 0);

            if (0 == strcmp(opal_info_path_prefix, scope)) {
                opal_info_show_path(opal_info_path_prefix, opal_install_dirs.prefix);
            } else if (0 == strcmp(opal_info_path_bindir, scope)) {
                opal_info_show_path(opal_info_path_bindir, opal_install_dirs.bindir);
            } else if (0 == strcmp(opal_info_path_libdir, scope)) {
                opal_info_show_path(opal_info_path_libdir, opal_install_dirs.libdir);
            } else if (0 == strcmp(opal_info_path_incdir, scope)) {
                opal_info_show_path(opal_info_path_incdir, opal_install_dirs.includedir);
            } else if (0 == strcmp(opal_info_path_mandir, scope)) {
                opal_info_show_path(opal_info_path_mandir, opal_install_dirs.mandir);
            } else if (0 == strcmp(opal_info_path_pkglibdir, scope)) {
                opal_info_show_path(opal_info_path_pkglibdir, opal_install_dirs.opallibdir);
            } else if (0 == strcmp(opal_info_path_sysconfdir, scope)) {
                opal_info_show_path(opal_info_path_sysconfdir, opal_install_dirs.sysconfdir);
            } else if (0 == strcmp(opal_info_path_exec_prefix, scope)) {
                opal_info_show_path(opal_info_path_exec_prefix, opal_install_dirs.exec_prefix);
            } else if (0 == strcmp(opal_info_path_sbindir, scope)) {
                opal_info_show_path(opal_info_path_sbindir, opal_install_dirs.sbindir);
            } else if (0 == strcmp(opal_info_path_libexecdir, scope)) {
                opal_info_show_path(opal_info_path_libexecdir, opal_install_dirs.libexecdir);
            } else if (0 == strcmp(opal_info_path_datarootdir, scope)) {
                opal_info_show_path(opal_info_path_datarootdir, opal_install_dirs.datarootdir);
            } else if (0 == strcmp(opal_info_path_datadir, scope)) {
                opal_info_show_path(opal_info_path_datadir, opal_install_dirs.datadir);
            } else if (0 == strcmp(opal_info_path_sharedstatedir, scope)) {
                opal_info_show_path(opal_info_path_sharedstatedir, opal_install_dirs.sharedstatedir);
            } else if (0 == strcmp(opal_info_path_localstatedir, scope)) {
                opal_info_show_path(opal_info_path_localstatedir, opal_install_dirs.localstatedir);
            } else if (0 == strcmp(opal_info_path_infodir, scope)) {
                opal_info_show_path(opal_info_path_infodir, opal_install_dirs.infodir);
            } else if (0 == strcmp(opal_info_path_pkgdatadir, scope)) {
                opal_info_show_path(opal_info_path_pkgdatadir, opal_install_dirs.opaldatadir);
            } else if (0 == strcmp(opal_info_path_pkgincludedir, scope)) {
                opal_info_show_path(opal_info_path_pkgincludedir, opal_install_dirs.opalincludedir);
            } else {
                char *usage = opal_cmd_line_get_usage_msg(cmd_line);
                opal_show_help("help-opal_info.txt", "usage", true, usage);
                free(usage);
                exit(1);
            }
        }
    }
}

void opal_info_do_params(bool want_all_in, bool want_internal,
                         opal_pointer_array_t *mca_types,
                         opal_pointer_array_t *component_map,
                         opal_cmd_line_t *opal_info_cmd_line)
{
    mca_base_var_info_lvl_t max_level = OPAL_INFO_LVL_1;
    int count;
    char *type, *component, *str;
    bool found;
    int i;
    bool want_all = false;
    char *p;

    if (opal_cmd_line_is_taken(opal_info_cmd_line, "param")) {
        p = "param";
    } else if (opal_cmd_line_is_taken(opal_info_cmd_line, "params")) {
        p = "params";
    } else {
        p = "foo";  /* should never happen, but protect against segfault */
    }

    if (NULL != (str = opal_cmd_line_get_param (opal_info_cmd_line, "level", 0, 0))) {
        char *tmp;

        errno = 0;
        max_level = strtol (str, &tmp, 10) + OPAL_INFO_LVL_1 - 1;
        if (0 != errno || '\0' != tmp[0] || max_level < OPAL_INFO_LVL_1 || max_level > OPAL_INFO_LVL_9) {
            char *usage = opal_cmd_line_get_usage_msg(opal_info_cmd_line);
            opal_show_help("help-opal_info.txt", "invalid-level", true, str);
            free(usage);
            exit(1);
        }
    } else if (want_all_in) {
        /* if not specified default to level 9 if all components are requested */
        max_level = OPAL_INFO_LVL_9;
    }

    if (want_all_in) {
        want_all = true;
    } else {
        /* See if the special param "all" was given to --param; that
         * supercedes any individual type
         */
        count = opal_cmd_line_get_ninsts(opal_info_cmd_line, p);
        for (i = 0; i < count; ++i) {
            type = opal_cmd_line_get_param(opal_info_cmd_line, p, (int)i, 0);
            if (0 == strcmp(opal_info_type_all, type)) {
                want_all = true;
                break;
            }
        }
    }

    /* Show the params */

    if (want_all) {
        opal_info_show_component_version(mca_types, component_map, opal_info_type_all,
                                         opal_info_component_all, opal_info_ver_full,
                                         opal_info_ver_all);
        for (i = 0; i < mca_types->size; ++i) {
            if (NULL == (type = (char *)opal_pointer_array_get_item(mca_types, i))) {
                continue;
            }
            opal_info_show_mca_params(type, opal_info_component_all, max_level, want_internal);
        }
    } else {
        for (i = 0; i < count; ++i) {
            type = opal_cmd_line_get_param(opal_info_cmd_line, p, (int)i, 0);
            component = opal_cmd_line_get_param(opal_info_cmd_line, p, (int)i, 1);

            for (found = false, i = 0; i < mca_types->size; ++i) {
                if (NULL == (str = (char *)opal_pointer_array_get_item(mca_types, i))) {
                    continue;
                }
                if (0 == strcmp(str, type)) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                char *usage = opal_cmd_line_get_usage_msg(opal_info_cmd_line);
                opal_show_help("help-opal_info.txt", "not-found", true, type);
                free(usage);
                exit(1);
            }

            opal_info_show_component_version(mca_types, component_map, type,
                                             component, opal_info_ver_full,
                                             opal_info_ver_all);
            opal_info_show_mca_params(type, component, max_level, want_internal);
        }
    }
}

void opal_info_err_params(opal_pointer_array_t *component_map)
{
    opal_info_component_map_t *map=NULL, *mptr;
    int i;

    /* all we want to do is display the LAST entry in the
     * component_map array as this is the one that generated the error
     */
    for (i=0; i < component_map->size; i++) {
        if (NULL == (mptr = (opal_info_component_map_t*)opal_pointer_array_get_item(component_map, i))) {
            continue;
        }
        map = mptr;
    }
    if (NULL == map) {
        fprintf(stderr, "opal_info_err_params: map not found\n");
        return;
    }
    opal_info_show_mca_params(map->type, opal_info_component_all, OPAL_INFO_LVL_9, true);
    fprintf(stderr, "\n");
    return;
}

void opal_info_do_type(opal_cmd_line_t *opal_info_cmd_line)
{
    mca_base_var_info_lvl_t max_level = OPAL_INFO_LVL_1;
    int count;
    char *type, *str;
    int i, j, k, len, ret;
    char *p;
    const mca_base_var_t *var;
    char** strings, *message;
    const mca_base_var_group_t *group;
    p = "type";

    if (NULL != (str = opal_cmd_line_get_param (opal_info_cmd_line, "level", 0, 0))) {
        char *tmp;

        errno = 0;
        max_level = strtol (str, &tmp, 10) + OPAL_INFO_LVL_1 - 1;
        if (0 != errno || '\0' != tmp[0] || max_level < OPAL_INFO_LVL_1 || max_level > OPAL_INFO_LVL_9) {
            char *usage = opal_cmd_line_get_usage_msg(opal_info_cmd_line);
            opal_show_help("help-opal_info.txt", "invalid-level", true, str);
            free(usage);
            exit(1);
        }
    }

    count = opal_cmd_line_get_ninsts(opal_info_cmd_line, p);
    len = mca_base_var_get_count ();

    for (k = 0; k < count; ++k) {
        type = opal_cmd_line_get_param(opal_info_cmd_line, p, k, 0);
        for (i = 0; i < len; ++i) {
            ret = mca_base_var_get (i, &var);
            if (OPAL_SUCCESS != ret) {
                continue;
            }
            if (0 == strcmp(type, ompi_var_type_names[var->mbv_type]) && (var->mbv_info_lvl <= max_level)) {
                ret = mca_base_var_dump(var->mbv_index, &strings, !opal_info_pretty ? MCA_BASE_VAR_DUMP_PARSABLE : MCA_BASE_VAR_DUMP_READABLE);
                if (OPAL_SUCCESS != ret) {
                    continue;
                }
                (void) mca_base_var_group_get(var->mbv_group_index, &group);
                for (j = 0 ; strings[j] ; ++j) {
                    if (0 == j && opal_info_pretty) {
                        asprintf (&message, "MCA %s", group->group_framework);
                        opal_info_out(message, message, strings[j]);
                        free(message);
                    } else {
                        opal_info_out("", "", strings[j]);
                    }
                    free(strings[j]);
                }
                free(strings);
            }
        }
    }
}

static void opal_info_show_mca_group_params(const mca_base_var_group_t *group, mca_base_var_info_lvl_t max_level, bool want_internal)
{
    const int *variables, *groups;
    const mca_base_pvar_t *pvar;
    const char *group_component;
    const mca_base_var_t *var;
    char **strings, *message;
    bool requested = true;
    int ret, i, j, count;

    variables = OPAL_VALUE_ARRAY_GET_BASE(&group->group_vars, const int);
    count = opal_value_array_get_size((opal_value_array_t *)&group->group_vars);

    /* the default component name is "base". depending on how the
     * group was registered the group may or not have this set.  */
    group_component = group->group_component ? group->group_component : "base";

    /* check if this group may be disabled due to a selection variable */
    if (0 != strcmp (group_component, "base")) {
        int var_id;

        /* read the selection parameter */
        var_id = mca_base_var_find (group->group_project, group->group_framework, NULL, NULL);
        if (0 <= var_id) {
            const mca_base_var_storage_t *value=NULL;
            char **requested_components;
            bool include_mode;

            mca_base_var_get_value (var_id, &value, NULL, NULL);
            if (NULL != value && NULL != value->stringval && '\0' != value->stringval[0]) {
                mca_base_component_parse_requested (value->stringval, &include_mode, &requested_components);

                for (i = 0, requested = !include_mode ; requested_components[i] ; ++i) {
                    if (0 == strcmp (requested_components[i], group_component)) {
                        requested = include_mode;
                        break;
                    }
                }

                opal_argv_free (requested_components);
            }
        }
    }

    const mca_base_var_group_t *curr_group = NULL;
    char *component_msg = NULL;
    asprintf(&component_msg, " %s", group_component);

    for (i = 0 ; i < count ; ++i) {
        ret = mca_base_var_get(variables[i], &var);
        if (OPAL_SUCCESS != ret || ((var->mbv_flags & MCA_BASE_VAR_FLAG_INTERNAL) &&
                                    !want_internal) ||
            max_level < var->mbv_info_lvl) {
            continue;
        }

        if (opal_info_pretty && curr_group != group) {
            asprintf(&message, "MCA%s %s%s", requested ? "" : " (-)",
                     group->group_framework,
                     component_msg ? component_msg : "");
            opal_info_out(message, message, "---------------------------------------------------");
            free(message);
            curr_group = group;
        }

        ret = mca_base_var_dump(variables[i], &strings, !opal_info_pretty ? MCA_BASE_VAR_DUMP_PARSABLE : MCA_BASE_VAR_DUMP_READABLE);
        if (OPAL_SUCCESS != ret) {
            continue;
        }

        for (j = 0 ; strings[j] ; ++j) {
            if (0 == j && opal_info_pretty) {
                asprintf (&message, "MCA%s %s%s", requested ? "" : " (-)",
                          group->group_framework,
                          component_msg ? component_msg : "");
                opal_info_out(message, message, strings[j]);
                free(message);
            } else {
                opal_info_out("", "", strings[j]);
            }
            free(strings[j]);
        }
        if (!opal_info_pretty) {
            /* generate an entry indicating whether this variable is disabled or not. if the
             * format in mca_base_var/pvar.c changes this needs to be changed as well */
            asprintf (&message, "mca:%s:%s:param:%s:disabled:%s", group->group_framework,
                      group_component, var->mbv_full_name, requested ? "false" : "true");
            opal_info_out("", "", message);
            free (message);
        }
        free(strings);
    }

    variables = OPAL_VALUE_ARRAY_GET_BASE(&group->group_pvars, const int);
    count = opal_value_array_get_size((opal_value_array_t *)&group->group_pvars);

    for (i = 0 ; i < count ; ++i) {
        ret = mca_base_pvar_get(variables[i], &pvar);
        if (OPAL_SUCCESS != ret || max_level < pvar->verbosity) {
            continue;
        }

        if (opal_info_pretty && curr_group != group) {
            asprintf(&message, "MCA%s %s%s", requested ? "" : " (-)",
                     group->group_framework,
                     component_msg ? component_msg : "");
            opal_info_out(message, message, "---------------------------------------------------");
            free(message);
            curr_group = group;
        }

        ret = mca_base_pvar_dump (variables[i], &strings, !opal_info_pretty ? MCA_BASE_VAR_DUMP_PARSABLE : MCA_BASE_VAR_DUMP_READABLE);
        if (OPAL_SUCCESS != ret) {
            continue;
        }

        for (j = 0 ; strings[j] ; ++j) {
            if (0 == j && opal_info_pretty) {
                asprintf (&message, "MCA%s %s%s", requested ? "" : " (-)",
                          group->group_framework,
                          component_msg ? component_msg : "");
                opal_info_out(message, message, strings[j]);
                free(message);
            } else {
                opal_info_out("", "", strings[j]);
            }
            free(strings[j]);
        }
        if (!opal_info_pretty) {
            /* generate an entry indicating whether this variable is disabled or not. if the
             * format in mca_base_var/pvar.c changes this needs to be changed as well */
            asprintf (&message, "mca:%s:%s:pvar:%s:disabled:%s", group->group_framework,
                      group_component, pvar->name, requested ? "false" : "true");
            opal_info_out("", "", message);
            free (message);
        }
        free(strings);
    }

    groups = OPAL_VALUE_ARRAY_GET_BASE(&group->group_subgroups, const int);
    count = opal_value_array_get_size((opal_value_array_t *)&group->group_subgroups);

    for (i = 0 ; i < count ; ++i) {
        ret = mca_base_var_group_get(groups[i], &group);
        if (OPAL_SUCCESS != ret) {
            continue;
        }
        opal_info_show_mca_group_params(group, max_level, want_internal);
    }
    free(component_msg);
}

void opal_info_show_mca_params(const char *type, const char *component,
                               mca_base_var_info_lvl_t max_level, bool want_internal)
{
    const mca_base_var_group_t *group;
    int ret;

    if (0 == strcmp (component, "all")) {
        ret = mca_base_var_group_find("*", type, NULL);
        if (0 > ret) {
            return;
        }

        (void) mca_base_var_group_get(ret, &group);

        opal_info_show_mca_group_params(group, max_level, want_internal);
    } else {
        ret = mca_base_var_group_find("*", type, component);
        if (0 > ret) {
            return;
        }

        (void) mca_base_var_group_get(ret, &group);
        opal_info_show_mca_group_params(group, max_level, want_internal);
    }
}



void opal_info_do_arch()
{
    opal_info_out("Configured architecture", "config:arch", OPAL_ARCH);
}


void opal_info_do_hostname()
{
    opal_info_out("Configure host", "config:host", OPAL_CONFIGURE_HOST);
}


static char *escape_quotes(const char *value)
{
    const char *src;
    int num_quotes = 0;
    for (src = value; src != NULL && *src != '\0'; ++src) {
        if ('"' == *src) {
            ++num_quotes;
        }
    }

    // If there are no quotes in the string, there's nothing to do
    if (0 == num_quotes) {
        return NULL;
    }

    // If we have quotes, make a new string.  Copy over the old
    // string, escaping the quotes along the way.  This is simple and
    // clear to read; it's not particularly efficient (performance is
    // definitely not important here).
    char *quoted_value;
    quoted_value = calloc(1, strlen(value) + num_quotes + 1);
    if (NULL == quoted_value) {
        return NULL;
    }

    char *dest;
    for (src = value, dest = quoted_value; *src != '\0'; ++src, ++dest) {
        if ('"' == *src) {
            *dest++ = '\\';
        }
        *dest = *src;
    }

    return quoted_value;
}


/*
 * Private variables - set some reasonable screen size defaults
 */

static int centerpoint = 24;
static int screen_width = 78;

/*
 * Prints the passed message in a pretty or parsable format.
 */
void opal_info_out(const char *pretty_message, const char *plain_message, const char *value)
{
    size_t len, max_value_width, value_offset;
    char *spaces = NULL;
    char *filler = NULL;
    char *pos, *v, savev, *v_to_free;

#ifdef HAVE_ISATTY
    /* If we have isatty(), if this is not a tty, then disable
     * wrapping for grep-friendly behavior
     */
    if (0 == isatty(STDOUT_FILENO)) {
        screen_width = INT_MAX;
    }
#endif

#ifdef TIOCGWINSZ
    if (screen_width < INT_MAX) {
        struct winsize size;
        if (ioctl(STDOUT_FILENO, TIOCGWINSZ, (char*) &size) >= 0) {
            screen_width = size.ws_col;
        }
    }
#endif

    /* Sanity check (allow NULL to mean "") */
    if (NULL == value) {
        value = "";
    }

    /* Strip leading and trailing whitespace from the string value */
    value_offset = strspn(value, " ");

    v = v_to_free = strdup(value + value_offset);
    len = strlen(v);

    if (len > 0) {
        while (len > 0 && isspace(v[len-1])) len--;
        v[len] = '\0';
    }

    if (opal_info_pretty && NULL != pretty_message) {
        if (centerpoint > (int)strlen(pretty_message)) {
            asprintf(&spaces, "%*s", centerpoint -
                     (int)strlen(pretty_message), " ");
        } else {
            spaces = strdup("");
#if OPAL_ENABLE_DEBUG
            if (centerpoint < (int)strlen(pretty_message)) {
                opal_show_help("help-opal_info.txt",
                               "developer warning: field too long", false,
                               pretty_message, centerpoint);
            }
#endif
        }
        max_value_width = screen_width - strlen(spaces) - strlen(pretty_message) - 2;
        if (0 < strlen(pretty_message)) {
            asprintf(&filler, "%s%s: ", spaces, pretty_message);
        } else {
            asprintf(&filler, "%s  ", spaces);
        }
        free(spaces);
        spaces = NULL;

        while (true) {
            if (strlen(v) < max_value_width) {
                printf("%s%s\n", filler, v);
                break;
            } else {
                asprintf(&spaces, "%*s", centerpoint + 2, " ");

                /* Work backwards to find the first space before
                 * max_value_width
                 */
                savev = v[max_value_width];
                v[max_value_width] = '\0';
                pos = (char*)strrchr(v, (int)' ');
                v[max_value_width] = savev;
                if (NULL == pos) {
                    /* No space found < max_value_width.  Look for the first
                     * space after max_value_width.
                     */
                    pos = strchr(&v[max_value_width], ' ');

                    if (NULL == pos) {

                        /* There's just no spaces.  So just print it and be done. */

                        printf("%s%s\n", filler, v);
                        break;
                    } else {
                        *pos = '\0';
                        printf("%s%s\n", filler, v);
                        v = pos + 1;
                    }
                } else {
                    *pos = '\0';
                    printf("%s%s\n", filler, v);
                    v = pos + 1;
                }

                /* Reset for the next iteration */
                free(filler);
                filler = strdup(spaces);
                free(spaces);
                spaces = NULL;
            }
        }
        if (NULL != filler) {
            free(filler);
        }
        if (NULL != spaces) {
            free(spaces);
        }
    } else {
        if (NULL != plain_message && 0 < strlen(plain_message)) {
            // Escape any double quotes in the value.
            char *quoted_value;
            quoted_value = escape_quotes(value);
            if (NULL != quoted_value) {
                value = quoted_value;
            }

            char *colon = strchr(value, ':');
            if (NULL != colon) {
                printf("%s:\"%s\"\n", plain_message, value);
            } else {
                printf("%s:%s\n", plain_message, value);
            }

            if (NULL != quoted_value) {
                free(quoted_value);
            }
        } else {
            printf("%s\n", value);
        }
    }
    if (NULL != v_to_free) {
        free(v_to_free);
    }
}

/*
 * Prints the passed integer in a pretty or parsable format.
 */
void opal_info_out_int(const char *pretty_message,
                       const char *plain_message,
                       int value)
{
    char *valstr;

    asprintf(&valstr, "%d", (int)value);
    opal_info_out(pretty_message, plain_message, valstr);
    free(valstr);
}

/*
 * Show all the components of a specific type/component combo (component may be
 * a wildcard)
 */
void opal_info_show_component_version(opal_pointer_array_t *mca_types,
                                      opal_pointer_array_t *component_map,
                                      const char *type_name,
                                      const char *component_name,
                                      const char *scope, const char *ver_type)
{
    bool want_all_components = false;
    bool want_all_types = false;
    bool found;
    mca_base_component_list_item_t *cli;
    mca_base_failed_component_t *cli_failed;
    int j;
    char *pos;
    opal_info_component_map_t *map;

    /* see if all components wanted */
    if (0 == strcmp(opal_info_component_all, component_name)) {
        want_all_components = true;
    }

    /* see if all types wanted */
    if (0 != strcmp(opal_info_type_all, type_name)) {
        /* Check to see if the type is valid */

        for (found = false, j = 0; j < mca_types->size; ++j) {
            if (NULL == (pos = (char*)opal_pointer_array_get_item(mca_types, j))) {
                continue;
            }
            if (0 == strcmp(pos, type_name)) {
                found = true;
                break;
            }
        }

        if (!found) {
            return;
        }
    } else {
        want_all_types = true;
    }

    /* Now that we have a valid type, find the right components */
    for (j=0; j < component_map->size; j++) {
        if (NULL == (map = (opal_info_component_map_t*)opal_pointer_array_get_item(component_map, j))) {
            continue;
        }
        if ((want_all_types || 0 == strcmp(type_name, map->type)) && map->components) {
            /* found it! */
            OPAL_LIST_FOREACH(cli, map->components, mca_base_component_list_item_t) {
                const mca_base_component_t *component = cli->cli_component;
                if (want_all_components ||
                    0 == strcmp(component->mca_component_name, component_name)) {
                    opal_info_show_mca_version(component, scope, ver_type);
                }
            }

            /* found it! */
            OPAL_LIST_FOREACH(cli_failed, map->failed_components, mca_base_failed_component_t) {
                mca_base_component_repository_item_t *ri = cli_failed->comp;
                if (want_all_components ||
                    0 == strcmp(component_name, ri->ri_name) ) {
                    opal_info_show_failed_component(ri, cli_failed->error_msg);
                }
            }

            if (!want_all_types) {
                break;
            }
        }
    }
}


static void opal_info_show_failed_component(const mca_base_component_repository_item_t* ri,
                                            const char *error_msg)
{
    char *message, *content;

    if (opal_info_pretty) {
        asprintf(&message, "MCA %s", ri->ri_type);
        asprintf(&content, "%s (failed to load) %s", ri->ri_name, error_msg);

        opal_info_out(message, NULL, content);

        free(message);
        free(content);
    } else {
        asprintf(&message, "mca:%s:%s:failed", ri->ri_type, ri->ri_name);
        asprintf(&content, "%s", error_msg);

        opal_info_out(NULL, message, content);

        free(message);
        free(content);
    }
}

/*
 * Given a component, display its relevant version(s)
 */
void opal_info_show_mca_version(const mca_base_component_t* component,
                                const char *scope, const char *ver_type)
{
    bool printed;
    bool want_mca = false;
    bool want_type = false;
    bool want_component = false;
    char *message, *content;
    char *mca_version;
    char *api_version;
    char *component_version;
    char *tmp;

    if (0 == strcmp(ver_type, opal_info_ver_all) ||
        0 == strcmp(ver_type, opal_info_ver_mca)) {
        want_mca = true;
    }

    if (0 == strcmp(ver_type, opal_info_ver_all) ||
        0 == strcmp(ver_type, opal_info_ver_type)) {
        want_type = true;
    }

    if (0 == strcmp(ver_type, opal_info_ver_all) ||
        0 == strcmp(ver_type, opal_info_ver_component)) {
        want_component = true;
    }

    mca_version = opal_info_make_version_str(scope, component->mca_major_version,
                                             component->mca_minor_version,
                                             component->mca_release_version, "",
                                             "");
    api_version = opal_info_make_version_str(scope, component->mca_type_major_version,
                                             component->mca_type_minor_version,
                                             component->mca_type_release_version, "",
                                             "");
    component_version = opal_info_make_version_str(scope, component->mca_component_major_version,
                                                   component->mca_component_minor_version,
                                                   component->mca_component_release_version,
                                                   "", "");
    if (opal_info_pretty) {
        asprintf(&message, "MCA %s", component->mca_type_name);
        printed = false;
        asprintf(&content, "%s (", component->mca_component_name);

        if (want_mca) {
            asprintf(&tmp, "%sMCA v%s", content, mca_version);
            free(content);
            content = tmp;
            printed = true;
        }

        if (want_type) {
            if (printed) {
                asprintf(&tmp, "%s, ", content);
                free(content);
                content = tmp;
            }
            asprintf(&tmp, "%sAPI v%s", content, api_version);
            free(content);
            content = tmp;
            printed = true;
        }

        if (want_component) {
            if (printed) {
                asprintf(&tmp, "%s, ", content);
                free(content);
                content = tmp;
            }
            asprintf(&tmp, "%sComponent v%s", content, component_version);
            free(content);
            content = tmp;
            printed = true;
        }
        if (NULL != content) {
            asprintf(&tmp, "%s)", content);
            free(content);
        } else {
            tmp = NULL;
        }

        opal_info_out(message, NULL, tmp);
        free(message);
        if (NULL != tmp) {
            free(tmp);
        }

    } else {
        asprintf(&message, "mca:%s:%s:version", component->mca_type_name, component->mca_component_name);
        if (want_mca) {
            asprintf(&tmp, "mca:%s", mca_version);
            opal_info_out(NULL, message, tmp);
            free(tmp);
        }
        if (want_type) {
            asprintf(&tmp, "api:%s", api_version);
            opal_info_out(NULL, message, tmp);
            free(tmp);
        }
        if (want_component) {
            asprintf(&tmp, "component:%s", component_version);
            opal_info_out(NULL, message, tmp);
            free(tmp);
        }
        free(message);
    }

    if (NULL != mca_version) {
        free(mca_version);
    }
    if (NULL != api_version) {
        free(api_version);
    }
    if (NULL != component_version) {
        free(component_version);
    }
}


char *opal_info_make_version_str(const char *scope,
                                 int major, int minor, int release,
                                 const char *greek,
                                 const char *repo)
{
    char *str = NULL, *tmp;
    char temp[BUFSIZ];

    temp[BUFSIZ - 1] = '\0';
    if (0 == strcmp(scope, opal_info_ver_full) ||
        0 == strcmp(scope, opal_info_ver_all)) {
        snprintf(temp, BUFSIZ - 1, "%d.%d.%d", major, minor, release);
        str = strdup(temp);
        if (NULL != greek) {
            asprintf(&tmp, "%s%s", str, greek);
            free(str);
            str = tmp;
        }
    } else if (0 == strcmp(scope, opal_info_ver_major)) {
        snprintf(temp, BUFSIZ - 1, "%d", major);
    } else if (0 == strcmp(scope, opal_info_ver_minor)) {
        snprintf(temp, BUFSIZ - 1, "%d", minor);
    } else if (0 == strcmp(scope, opal_info_ver_release)) {
        snprintf(temp, BUFSIZ - 1, "%d", release);
    } else if (0 == strcmp(scope, opal_info_ver_greek)) {
        str = strdup(greek);
    } else if (0 == strcmp(scope, opal_info_ver_repo)) {
        str = strdup(repo);
    }

    if (NULL == str) {
        str = strdup(temp);
    }

    return str;
}

void opal_info_show_opal_version(const char *scope)
{
    char *tmp, *tmp2;

    asprintf(&tmp, "%s:version:full", opal_info_type_opal);
    tmp2 = opal_info_make_version_str(scope,
                                      OPAL_MAJOR_VERSION, OPAL_MINOR_VERSION,
                                      OPAL_RELEASE_VERSION,
                                      OPAL_GREEK_VERSION,
                                      OPAL_REPO_REV);
    opal_info_out("OPAL", tmp, tmp2);
    free(tmp);
    free(tmp2);
    asprintf(&tmp, "%s:version:repo", opal_info_type_opal);
    opal_info_out("OPAL repo revision", tmp, OPAL_REPO_REV);
    free(tmp);
    asprintf(&tmp, "%s:version:release_date", opal_info_type_opal);
    opal_info_out("OPAL release date", tmp, OPAL_RELEASE_DATE);
    free(tmp);
}
