/*
 * Copyright (c) 2004-2009 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2015 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2015-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"

#include <string.h>
#include <ctype.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif
#ifdef HAVE_NETDB_H
#include <netdb.h>
#endif

#include MCA_timer_IMPLEMENTATION_HEADER
#include "opal/include/opal/version.h"
#include "opal/mca/installdirs/installdirs.h"
#include "opal/class/opal_value_array.h"
#include "opal/class/opal_pointer_array.h"
#include "opal/util/printf.h"
#include "opal/memoryhooks/memory.h"
#include "opal/opal_portable_platform.h"

#include "orte/util/show_help.h"


#include "orte/tools/orte-info/orte-info.h"


/*
 * Public variables
 */

const char *orte_info_component_all = "all";
const char *orte_info_param_all = "all";

const char *orte_info_path_prefix = "prefix";
const char *orte_info_path_bindir = "bindir";
const char *orte_info_path_libdir = "libdir";
const char *orte_info_path_incdir = "incdir";
const char *orte_info_path_mandir = "mandir";
const char *orte_info_path_pkglibdir = "pkglibdir";
const char *orte_info_path_sysconfdir = "sysconfdir";
const char *orte_info_path_exec_prefix = "exec_prefix";
const char *orte_info_path_sbindir = "sbindir";
const char *orte_info_path_libexecdir = "libexecdir";
const char *orte_info_path_datarootdir = "datarootdir";
const char *orte_info_path_datadir = "datadir";
const char *orte_info_path_sharedstatedir = "sharedstatedir";
const char *orte_info_path_localstatedir = "localstatedir";
const char *orte_info_path_infodir = "infodir";
const char *orte_info_path_pkgdatadir = "pkgdatadir";
const char *orte_info_path_pkgincludedir = "pkgincludedir";

void orte_info_do_params(bool want_all_in, bool want_internal)
{
    int count;
    char *type, *component, *str;
    bool found;
    int i;
    bool want_all = false;

    orte_info_components_open();

    if (want_all_in) {
        want_all = true;
    } else {
        /* See if the special param "all" was givin to --param; that
         * superceeds any individual type
         */
        count = opal_cmd_line_get_ninsts(orte_info_cmd_line, "param");
        for (i = 0; i < count; ++i) {
            type = opal_cmd_line_get_param(orte_info_cmd_line, "param", (int)i, 0);
            if (0 == strcmp(orte_info_type_all, type)) {
                want_all = true;
                break;
            }
        }
    }

    /* Show the params */
    if (want_all) {
        for (i = 0; i < mca_types.size; ++i) {
            if (NULL == (type = (char *)opal_pointer_array_get_item(&mca_types, i))) {
                continue;
            }
            orte_info_show_mca_params(type, orte_info_component_all, want_internal);
        }
    } else {
        for (i = 0; i < count; ++i) {
            type = opal_cmd_line_get_param(orte_info_cmd_line, "param", (int)i, 0);
            component = opal_cmd_line_get_param(orte_info_cmd_line, "param", (int)i, 1);

            for (found = false, i = 0; i < mca_types.size; ++i) {
                if (NULL == (str = (char *)opal_pointer_array_get_item(&mca_types, i))) {
                    continue;
                }
                if (0 == strcmp(str, type)) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                char *usage = opal_cmd_line_get_usage_msg(orte_info_cmd_line);
                orte_show_help("help-orte-info.txt", "not-found", true, type);
                free(usage);
                exit(1);
            }

            orte_info_show_mca_params(type, component, want_internal);
        }
    }
}

static void orte_info_show_mca_group_params(const mca_base_var_group_t *group, bool want_internal)
{
    const mca_base_var_t *var;
    const int *variables;
    int ret, i, j, count;
    const int *groups;
    char **strings;

    variables = OPAL_VALUE_ARRAY_GET_BASE(&group->group_vars, const int);
    count = opal_value_array_get_size((opal_value_array_t *)&group->group_vars);

    for (i = 0 ; i < count ; ++i) {
        ret = mca_base_var_get(variables[i], &var);
        if (OPAL_SUCCESS != ret || ((var->mbv_flags & MCA_BASE_VAR_FLAG_INTERNAL) &&
                                    !want_internal)) {
            continue;
        }

        ret = mca_base_var_dump(variables[i], &strings, !orte_info_pretty ? MCA_BASE_VAR_DUMP_PARSABLE : MCA_BASE_VAR_DUMP_READABLE);
        if (OPAL_SUCCESS != ret) {
            continue;
        }

        for (j = 0 ; strings[j] ; ++j) {
            if (0 == j && orte_info_pretty) {
                char *message;

                asprintf (&message, "MCA %s", group->group_framework);
                orte_info_out(message, message, strings[j]);
                free(message);
            } else {
                orte_info_out("", "", strings[j]);
            }
            free(strings[j]);
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
        orte_info_show_mca_group_params(group, want_internal);
    }
}

void orte_info_show_mca_params(const char *type, const char *component,
                               bool want_internal)
{
    const mca_base_var_group_t *group;
    int ret;

    if (0 == strcmp (component, "all")) {
        ret = mca_base_var_group_find("*", type, NULL);
        if (0 > ret) {
            return;
        }

        (void) mca_base_var_group_get(ret, &group);

        orte_info_show_mca_group_params(group, want_internal);
    } else {
        ret = mca_base_var_group_find("*", type, component);
        if (0 > ret) {
            return;
        }

        (void) mca_base_var_group_get(ret, &group);
        orte_info_show_mca_group_params(group, want_internal);
    }
}

void orte_info_do_path(bool want_all, opal_cmd_line_t *cmd_line)
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
        orte_info_show_path(orte_info_path_prefix, opal_install_dirs.prefix);
        orte_info_show_path(orte_info_path_exec_prefix, opal_install_dirs.exec_prefix);
        orte_info_show_path(orte_info_path_bindir, opal_install_dirs.bindir);
        orte_info_show_path(orte_info_path_sbindir, opal_install_dirs.sbindir);
        orte_info_show_path(orte_info_path_libdir, opal_install_dirs.libdir);
        orte_info_show_path(orte_info_path_incdir, opal_install_dirs.includedir);
        orte_info_show_path(orte_info_path_mandir, opal_install_dirs.mandir);
        orte_info_show_path(orte_info_path_pkglibdir, opal_install_dirs.opallibdir);
        orte_info_show_path(orte_info_path_libexecdir, opal_install_dirs.libexecdir);
        orte_info_show_path(orte_info_path_datarootdir, opal_install_dirs.datarootdir);
        orte_info_show_path(orte_info_path_datadir, opal_install_dirs.datadir);
        orte_info_show_path(orte_info_path_sysconfdir, opal_install_dirs.sysconfdir);
        orte_info_show_path(orte_info_path_sharedstatedir, opal_install_dirs.sharedstatedir);
        orte_info_show_path(orte_info_path_localstatedir, opal_install_dirs.localstatedir);
        orte_info_show_path(orte_info_path_infodir, opal_install_dirs.infodir);
        orte_info_show_path(orte_info_path_pkgdatadir, opal_install_dirs.opaldatadir);
        orte_info_show_path(orte_info_path_pkglibdir, opal_install_dirs.opallibdir);
        orte_info_show_path(orte_info_path_pkgincludedir, opal_install_dirs.opalincludedir);
    } else {
        count = opal_cmd_line_get_ninsts(cmd_line, "path");
        for (i = 0; i < count; ++i) {
            scope = opal_cmd_line_get_param(cmd_line, "path", i, 0);

            if (0 == strcmp(orte_info_path_prefix, scope)) {
                orte_info_show_path(orte_info_path_prefix, opal_install_dirs.prefix);
            } else if (0 == strcmp(orte_info_path_bindir, scope)) {
                orte_info_show_path(orte_info_path_bindir, opal_install_dirs.bindir);
            } else if (0 == strcmp(orte_info_path_libdir, scope)) {
                orte_info_show_path(orte_info_path_libdir, opal_install_dirs.libdir);
            } else if (0 == strcmp(orte_info_path_incdir, scope)) {
                orte_info_show_path(orte_info_path_incdir, opal_install_dirs.includedir);
            } else if (0 == strcmp(orte_info_path_mandir, scope)) {
                orte_info_show_path(orte_info_path_mandir, opal_install_dirs.mandir);
            } else if (0 == strcmp(orte_info_path_pkglibdir, scope)) {
                orte_info_show_path(orte_info_path_pkglibdir, opal_install_dirs.opallibdir);
            } else if (0 == strcmp(orte_info_path_sysconfdir, scope)) {
                orte_info_show_path(orte_info_path_sysconfdir, opal_install_dirs.sysconfdir);
            } else if (0 == strcmp(orte_info_path_exec_prefix, scope)) {
                orte_info_show_path(orte_info_path_exec_prefix, opal_install_dirs.exec_prefix);
            } else if (0 == strcmp(orte_info_path_sbindir, scope)) {
                orte_info_show_path(orte_info_path_sbindir, opal_install_dirs.sbindir);
            } else if (0 == strcmp(orte_info_path_libexecdir, scope)) {
                orte_info_show_path(orte_info_path_libexecdir, opal_install_dirs.libexecdir);
            } else if (0 == strcmp(orte_info_path_datarootdir, scope)) {
                orte_info_show_path(orte_info_path_datarootdir, opal_install_dirs.datarootdir);
            } else if (0 == strcmp(orte_info_path_datadir, scope)) {
                orte_info_show_path(orte_info_path_datadir, opal_install_dirs.datadir);
            } else if (0 == strcmp(orte_info_path_sharedstatedir, scope)) {
                orte_info_show_path(orte_info_path_sharedstatedir, opal_install_dirs.sharedstatedir);
            } else if (0 == strcmp(orte_info_path_localstatedir, scope)) {
                orte_info_show_path(orte_info_path_localstatedir, opal_install_dirs.localstatedir);
            } else if (0 == strcmp(orte_info_path_infodir, scope)) {
                orte_info_show_path(orte_info_path_infodir, opal_install_dirs.infodir);
            } else if (0 == strcmp(orte_info_path_pkgdatadir, scope)) {
                orte_info_show_path(orte_info_path_pkgdatadir, opal_install_dirs.opaldatadir);
            } else if (0 == strcmp(orte_info_path_pkgincludedir, scope)) {
                orte_info_show_path(orte_info_path_pkgincludedir, opal_install_dirs.opalincludedir);
            } else {
                char *usage = opal_cmd_line_get_usage_msg(cmd_line);
                orte_show_help("help-orte-info.txt", "usage", true, usage);
                free(usage);
                exit(1);
            }
        }
    }
}


void orte_info_show_path(const char *type, const char *value)
{
    char *pretty, *path;

    pretty = strdup(type);
    pretty[0] = toupper(pretty[0]);

    asprintf(&path, "path:%s", type);
    orte_info_out(pretty, path, value);
    free(pretty);
    free(path);
}


void orte_info_do_arch()
{
    orte_info_out("Configured architecture", "config:arch", OPAL_ARCH);
}


void orte_info_do_hostname()
{
    orte_info_out("Configure host", "config:host", OPAL_CONFIGURE_HOST);
}


/*
 * do_config
 * Accepts:
 *	- want_all: boolean flag; TRUE -> display all options
 *				  FALSE -> display selected options
 *
 * This function displays all the options with which the current
 * installation of orte was configured. There are many options here
 * that are carried forward from OMPI-7 and are not mca parameters
 * in OMPI-10. I have to dig through the invalid options and replace
 * them with OMPI-10 options.
 */
void orte_info_do_config(bool want_all)
{
    char *heterogeneous;
    char *memprofile;
    char *memdebug;
    char *debug;
    char *threads;
    char *have_dl;
    char *orterun_prefix_by_default;
    char *wtime_support;
    char *symbol_visibility;
    char *ft_support;

    /* setup the strings that don't require allocations*/
    heterogeneous = OPAL_ENABLE_HETEROGENEOUS_SUPPORT ? "yes" : "no";
    memprofile = OPAL_ENABLE_MEM_PROFILE ? "yes" : "no";
    memdebug = OPAL_ENABLE_MEM_DEBUG ? "yes" : "no";
    debug = OPAL_ENABLE_DEBUG ? "yes" : "no";
    have_dl = OPAL_HAVE_DL_SUPPORT ? "yes" : "no";
    orterun_prefix_by_default = ORTE_WANT_ORTERUN_PREFIX_BY_DEFAULT ? "yes" : "no";
    wtime_support = OPAL_TIMER_USEC_NATIVE ? "native" : "gettimeofday";
    symbol_visibility = OPAL_C_HAVE_VISIBILITY ? "yes" : "no";

    /* setup strings that require allocation */
    asprintf(&threads, "%s (OPAL: %s, ORTE progress: yes, Event lib: yes)",
             "posix",
             OPAL_ENABLE_MULTI_THREADS ? "yes" : "no");

    asprintf(&ft_support, "%s (checkpoint thread: %s)",
             OPAL_ENABLE_FT ? "yes" : "no", OPAL_ENABLE_FT_THREAD ? "yes" : "no");;

    /* output values */
    orte_info_out("Configured by", "config:user", OPAL_CONFIGURE_USER);
    orte_info_out("Configured on", "config:timestamp", OPAL_CONFIGURE_DATE);
    orte_info_out("Configure host", "config:host", OPAL_CONFIGURE_HOST);
    orte_info_out("Configure command line", "config:cli", OPAL_CONFIGURE_CLI);

    orte_info_out("Built by", "build:user", OMPI_BUILD_USER);
    orte_info_out("Built on", "build:timestamp", OMPI_BUILD_DATE);
    orte_info_out("Built host", "build:host", OMPI_BUILD_HOST);

    orte_info_out("C compiler", "compiler:c:command", OPAL_CC);
    orte_info_out("C compiler absolute", "compiler:c:absolute", OPAL_CC_ABSOLUTE);
    orte_info_out("C compiler family name", "compiler:c:familyname", _STRINGIFY(OPAL_BUILD_PLATFORM_COMPILER_FAMILYNAME));
    orte_info_out("C compiler version", "compiler:c:version", _STRINGIFY(OPAL_BUILD_PLATFORM_COMPILER_VERSION_STR));

    if (want_all) {
        orte_info_out_int("C char size", "compiler:c:sizeof:char", sizeof(char));
        /* JMS: should be fixed in MPI-2.2 to differentiate between C
         _Bool and C++ bool.  For the moment, the code base assumes
         that they are the same.  Because of opal_config_bottom.h,
         we can sizeof(bool) here, so we might as well -- even
         though this technically isn't right.  This should be fixed
         when we update to MPI-2.2.  See below for note about C++
         bool alignment. */
        orte_info_out_int("C bool size", "compiler:c:sizeof:bool", sizeof(bool));
        orte_info_out_int("C short size", "compiler:c:sizeof:short", sizeof(short));
        orte_info_out_int("C int size", "compiler:c:sizeof:int", sizeof(int));
        orte_info_out_int("C long size", "compiler:c:sizeof:long", sizeof(long));
        orte_info_out_int("C float size", "compiler:c:sizeof:float", sizeof(float));
        orte_info_out_int("C double size", "compiler:c:sizeof:double", sizeof(double));
        orte_info_out_int("C pointer size", "compiler:c:sizeof:pointer", sizeof(void *));
        orte_info_out_int("C char align", "compiler:c:align:char", OPAL_ALIGNMENT_CHAR);
        orte_info_out("C bool align", "compiler:c:align:bool", "skipped");
        orte_info_out_int("C int align", "compiler:c:align:int", OPAL_ALIGNMENT_INT);
        orte_info_out_int("C float align", "compiler:c:align:float", OPAL_ALIGNMENT_FLOAT);
        orte_info_out_int("C double align", "compiler:c:align:double", OPAL_ALIGNMENT_DOUBLE);
    }

    orte_info_out("Thread support", "option:threads", threads);

    if (want_all) {


        orte_info_out("Build CFLAGS", "option:build:cflags", OMPI_BUILD_CFLAGS);
        orte_info_out("Build LDFLAGS", "option:build:ldflags", OMPI_BUILD_LDFLAGS);
        orte_info_out("Build LIBS", "option:build:libs", OMPI_BUILD_LIBS);

        orte_info_out("Wrapper extra CFLAGS", "option:wrapper:extra_cflags",
                      WRAPPER_EXTRA_CFLAGS);
        orte_info_out("Wrapper extra LDFLAGS", "option:wrapper:extra_ldflags",
                      WRAPPER_EXTRA_LDFLAGS);
        orte_info_out("Wrapper extra LIBS", "option:wrapper:extra_libs",
                      WRAPPER_EXTRA_LIBS);
    }
    free(threads);

    orte_info_out("Internal debug support", "option:debug", debug);
    orte_info_out("Memory profiling support", "option:mem-profile", memprofile);
    orte_info_out("Memory debugging support", "option:mem-debug", memdebug);
    orte_info_out("dl support", "option:dlopen", have_dl);
    orte_info_out("Heterogeneous support", "options:heterogeneous", heterogeneous);
    orte_info_out("orterun default --prefix", "orterun:prefix_by_default",
                  orterun_prefix_by_default);
    orte_info_out("MPI_WTIME support", "options:mpi-wtime", wtime_support);
    orte_info_out("Symbol vis. support", "options:visibility", symbol_visibility);

    orte_info_out("FT Checkpoint support", "options:ft_support", ft_support);
    free(ft_support);

}
