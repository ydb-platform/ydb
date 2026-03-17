/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2016 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2007 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2011-2012 University of Houston. All rights reserved.
 * Copyright (c) 2010-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include <stdio.h>
#include <string.h>
#include <ctype.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_NETDB_H
#include <netdb.h>
#endif
#ifdef HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif
#include <errno.h>
#include <signal.h>

#include "opal/version.h"
#include "opal/mca/installdirs/installdirs.h"
#include "opal/class/opal_object.h"
#include "opal/class/opal_pointer_array.h"
#include "opal/runtime/opal.h"
#if OPAL_ENABLE_FT_CR == 1
#include "opal/runtime/opal_cr.h"
#endif
#include "opal/mca/base/base.h"
#include "opal/runtime/opal_info_support.h"
#include "opal/util/argv.h"
#include "opal/util/show_help.h"

#if OMPI_RTE_ORTE
#include "orte/runtime/orte_info_support.h"
#endif

#include "ompi/communicator/communicator.h"
#include "ompi/tools/ompi_info/ompi_info.h"
#include "ompi/runtime/ompi_info_support.h"

/*
 * Public variables
 */


int main(int argc, char *argv[])
{
    int ret = 0;
    bool acted = false;
    bool want_all = false;
    char **app_env = NULL, **global_env = NULL;
    int i;
    opal_cmd_line_t *ompi_info_cmd_line;
    opal_pointer_array_t mca_types;
    opal_pointer_array_t component_map;
    opal_info_component_map_t *map;

    /* protect against problems if someone passes us thru a pipe
     * and then abnormally terminates the pipe early */
    signal(SIGPIPE, SIG_IGN);

    /* Initialize the argv parsing handle */
    if (OPAL_SUCCESS != opal_init_util(&argc, &argv)) {
        opal_show_help("help-opal_info.txt", "lib-call-fail", true,
                       "opal_init_util", __FILE__, __LINE__, NULL);
        exit(ret);
    }

    ompi_info_cmd_line = OBJ_NEW(opal_cmd_line_t);
    if (NULL == ompi_info_cmd_line) {
        ret = errno;
        opal_show_help("help-opal_info.txt", "lib-call-fail", true,
                       "opal_cmd_line_create", __FILE__, __LINE__, NULL);
        exit(ret);
    }

    /* initialize the command line, parse it, and return the directives
     * telling us what the user wants output
     */
    if (OPAL_SUCCESS != (ret = opal_info_init(argc, argv, ompi_info_cmd_line))) {
        exit(ret);
    }

    if (opal_cmd_line_is_taken(ompi_info_cmd_line, "version")) {
        fprintf(stdout, "Open MPI v%s\n\n%s\n",
                OPAL_VERSION, PACKAGE_BUGREPORT);
        exit(0);
    }

    /* setup the mca_types array */
    OBJ_CONSTRUCT(&mca_types, opal_pointer_array_t);
    opal_pointer_array_init(&mca_types, 128, INT_MAX, 64);

    /* add in the opal frameworks */
    opal_info_register_types(&mca_types);

#if OMPI_RTE_ORTE
    /* add in the orte frameworks */
    orte_info_register_types(&mca_types);
#endif

    ompi_info_register_types(&mca_types);

    /* init the component map */
    OBJ_CONSTRUCT(&component_map, opal_pointer_array_t);
    opal_pointer_array_init(&component_map, 64, INT_MAX, 32);

    /* Register OMPI's params */
    if (OMPI_SUCCESS != (ret = ompi_info_register_framework_params(&component_map))) {
        if (OMPI_ERR_BAD_PARAM == ret) {
            /* output what we got */
            opal_info_do_params(true, opal_cmd_line_is_taken(ompi_info_cmd_line, "internal"),
                                &mca_types, &component_map, NULL);
        }
        exit(1);
    }

    /* Execute the desired action(s) */
    want_all = opal_cmd_line_is_taken(ompi_info_cmd_line, "all");
    if (want_all) {
        opal_info_out("Package", "package", OPAL_PACKAGE_STRING);
        ompi_info_show_ompi_version(opal_info_ver_full);
    }
    if (want_all || opal_cmd_line_is_taken(ompi_info_cmd_line, "path")) {
        opal_info_do_path(want_all, ompi_info_cmd_line);
        acted = true;
    }
    if (want_all || opal_cmd_line_is_taken(ompi_info_cmd_line, "arch")) {
        opal_info_do_arch();
        acted = true;
    }
    if (want_all || opal_cmd_line_is_taken(ompi_info_cmd_line, "hostname")) {
        opal_info_do_hostname();
        acted = true;
    }
    if (want_all || opal_cmd_line_is_taken(ompi_info_cmd_line, "config")) {
        ompi_info_do_config(true);
        acted = true;
    }
    if (want_all || opal_cmd_line_is_taken(ompi_info_cmd_line, "param") ||
        opal_cmd_line_is_taken(ompi_info_cmd_line, "params")) {
        opal_info_do_params(want_all, opal_cmd_line_is_taken(ompi_info_cmd_line, "internal"),
                            &mca_types, &component_map, ompi_info_cmd_line);
        acted = true;
    }
    if (opal_cmd_line_is_taken(ompi_info_cmd_line, "type")) {
        opal_info_do_type(ompi_info_cmd_line);
        acted = true;
    }

    /* If no command line args are specified, show default set */

    if (!acted) {
        opal_info_out("Package", "package", OPAL_PACKAGE_STRING);
        ompi_info_show_ompi_version(opal_info_ver_full);
        opal_info_show_path(opal_info_path_prefix, opal_install_dirs.prefix);
        opal_info_do_arch();
        opal_info_do_hostname();
        ompi_info_do_config(false);
        opal_info_show_component_version(&mca_types, &component_map, opal_info_type_all,
                                         opal_info_component_all, opal_info_ver_full,
                                         opal_info_ver_all);
    }

    /* All done */

    if (NULL != app_env) {
        opal_argv_free(app_env);
    }
    if (NULL != global_env) {
        opal_argv_free(global_env);
    }
    ompi_info_close_components();
    OBJ_RELEASE(ompi_info_cmd_line);
    OBJ_DESTRUCT(&mca_types);
    for (i=0; i < component_map.size; i++) {
        if (NULL != (map = (opal_info_component_map_t*)opal_pointer_array_get_item(&component_map, i))) {
            OBJ_RELEASE(map);
        }
    }
    OBJ_DESTRUCT(&component_map);

    opal_info_finalize();

    /* Put our own call to opal_finalize_util() here because we called
       it up above (and it refcounts) */
    opal_finalize_util();

    return 0;
}
