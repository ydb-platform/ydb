/* Error message processing */

#ifdef _MSC_VER
#ifndef _CRT_SECURE_NO_DEPRECATE
#define _CRT_SECURE_NO_DEPRECATE
#endif
#ifndef _CRT_NONSTDC_NO_DEPRECATE
#define _CRT_NONSTDC_NO_DEPRECATE
#endif
#endif

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "proj_config.h"
#include "proj_internal.h"
#define EMESS_ROUTINE
#include "emess.h"

void emess(int code, const char *fmt, ...) {
    va_list args;

    va_start(args, fmt);
    /* prefix program name, if given */
    if (emess_dat.Prog_name != nullptr) {
        // For unit test purposes, allow PROJ_DISPLAY_PROGRAM_NAME=NO
        const char *pszDisplayProgramName = getenv("PROJ_DISPLAY_PROGRAM_NAME");
        if (!(pszDisplayProgramName &&
              strcmp(pszDisplayProgramName, "NO") == 0)) {
            (void)fprintf(stderr, "%s\n<%s>: ", pj_get_release(),
                          emess_dat.Prog_name);
        }
    }
    /* print file name and line, if given */
    if (emess_dat.File_name != nullptr && *emess_dat.File_name) {
        (void)fprintf(stderr, "while processing file: %s", emess_dat.File_name);
        if (emess_dat.File_line > 0)
            (void)fprintf(stderr, ", line %d\n", emess_dat.File_line);
        else
            (void)fputc('\n', stderr);
    } else
        putc('\n', stderr);
    /* if |code|==2, print errno code data */
    if (code == 2 || code == -2) {
        int my_errno = errno;
#ifdef HAVE_STRERROR
        const char *my_strerror = strerror(my_errno);
#endif
#ifndef HAVE_STRERROR
        const char *my_strerror = "<system mess. texts unavail.>";
#endif
        (void)fprintf(stderr, "Sys errno: %d: %s\n", my_errno, my_strerror);
    }

    /* post remainder of call data */
    (void)vfprintf(stderr, fmt, args);
    va_end(args);
    /* die if code positive */
    if (code > 0) {
        (void)fputs("\nprogram abnormally terminated\n", stderr);
        exit(code);
    } else
        putc('\n', stderr);
}
