/*
 * Copyright (c) 2006-2007 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2007-2010 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2007      Sun Microsystem, Inc.  All rights reserved.
 * Copyright (c) 2010      Sandia National Laboratories. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

#include "opal_config.h"

#include <string.h>

#include "opal/util/os_path.h"
#include "opal/mca/installdirs/base/base.h"
#include "opal/mca/installdirs/installdirs.h"

/* Support both ${name} and @{name} forms.  The latter allows us to
   pass values through AC_SUBST without being munged by m4 (e.g., if
   we want to pass "@{libdir}" and not have it replaced by m4 to be
   whatever the actual value of the shell variable is. */
#define EXPAND_STRING(name) EXPAND_STRING2(name, name)

#define EXPAND_STRING2(ompiname, fieldname)                             \
    do {                                                                \
        if (NULL != (start_pos = strstr(retval, "${" #fieldname "}"))) { \
            tmp = retval;                                               \
            *start_pos = '\0';                                          \
            end_pos = start_pos + strlen("${" #fieldname "}");          \
            asprintf(&retval, "%s%s%s", tmp,                            \
                     opal_install_dirs.ompiname + destdir_offset,       \
                     end_pos);                                          \
            free(tmp);                                                  \
            changed = true;                                             \
        } else if (NULL != (start_pos = strstr(retval, "@{" #fieldname "}"))) { \
            tmp = retval;                                               \
            *start_pos = '\0';                                          \
            end_pos = start_pos + strlen("@{" #fieldname "}");          \
            asprintf(&retval, "%s%s%s", tmp,                            \
                     opal_install_dirs.ompiname + destdir_offset,       \
                     end_pos);                                          \
            free(tmp);                                                  \
            changed = true;                                             \
        }                                                               \
    } while (0)


/*
 * Read the lengthy comment below to understand the value of the
 * is_setup parameter.
 */
static char *
opal_install_dirs_expand_internal(const char* input, bool is_setup)
{
    size_t len, i;
    bool needs_expand = false;
    char *retval = NULL;
    char *destdir = NULL;
    size_t destdir_offset = 0;

    /* This is subtle, and worth explaining.

       If we substitute in any ${FIELD} values, we need to prepend it
       with the value of the $OPAL_DESTDIR environment variable -- if
       it is set.

       We need to handle at least three cases properly (assume that
       configure was invoked with --prefix=/opt/openmpi and no other
       directory specifications, and OPAL_DESTDIR is set to
       /tmp/buildroot):

       1. Individual directories, such as libdir.  These need to be
          prepended with DESTDIR.  I.e., return
          /tmp/buildroot/opt/openmpi/lib.

       2. Compiler flags that have ${FIELD} values embedded in them.
          For example, consider if a wrapper compiler data file
          contains the line:

          preprocessor_flags=-DMYFLAG="${prefix}/share/randomthingy/"

          The value we should return is:

          -DMYFLAG="/tmp/buildroot/opt/openmpi/share/randomthingy/"

       3. Compiler flags that do not have any ${FIELD} values.
          For example, consider if a wrapper compiler data file
          contains the line:

          preprocessor_flags=-pthread

          The value we should return is:

          -pthread

       Note, too, that this OPAL_DESTDIR futzing only needs to occur
       during opal_init().  By the time opal_init() has completed, all
       values should be substituted in that need substituting.  Hence,
       we take an extra parameter (is_setup) to know whether we should
       do this futzing or not. */
    if (is_setup) {
        destdir = getenv("OPAL_DESTDIR");
        if (NULL != destdir && strlen(destdir) > 0) {
            destdir_offset = strlen(destdir);
        }
    }

    len = strlen(input);
    for (i = 0 ; i < len ; ++i) {
        if ('$' == input[i] || '@' == input[i]) {
            needs_expand = true;
            break;
        }
    }

    retval = strdup(input);
    if (NULL == retval) return NULL;

    if (needs_expand) {
        bool changed = false;
        char *start_pos, *end_pos, *tmp;

        do {
            changed = false;
            EXPAND_STRING(prefix);
            EXPAND_STRING(exec_prefix);
            EXPAND_STRING(bindir);
            EXPAND_STRING(sbindir);
            EXPAND_STRING(libexecdir);
            EXPAND_STRING(datarootdir);
            EXPAND_STRING(datadir);
            EXPAND_STRING(sysconfdir);
            EXPAND_STRING(sharedstatedir);
            EXPAND_STRING(localstatedir);
            EXPAND_STRING(libdir);
            EXPAND_STRING(includedir);
            EXPAND_STRING(infodir);
            EXPAND_STRING(mandir);
            EXPAND_STRING2(opaldatadir, pkgdatadir);
            EXPAND_STRING2(opallibdir, pkglibdir);
            EXPAND_STRING2(opalincludedir, pkgincludedir);
        } while (changed);
    }

    if (NULL != destdir) {
        char *tmp = retval;
        retval = opal_os_path(false, destdir, tmp, NULL);
        free(tmp);
    }

    return retval;
}


char *
opal_install_dirs_expand(const char* input)
{
    /* We do NOT want OPAL_DESTDIR expansion in this case. */
    return opal_install_dirs_expand_internal(input, false);
}


char *
opal_install_dirs_expand_setup(const char* input)
{
    /* We DO want OPAL_DESTDIR expansion in this case. */
    return opal_install_dirs_expand_internal(input, true);
}
