/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/*-
 * Copyright (c) 1990, 1993
 *      The Regents of the University of California.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 4. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#include "opal_config.h"

#ifdef HAVE_SYS_CDEFS_H
# include <sys/cdefs.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#include <sys/stat.h>
#ifdef HAVE_SYS_IOCTL_H
#include <sys/ioctl.h>
#endif
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif
#ifdef HAVE_TERMIOS_H
# include <termios.h>
#else
# ifdef HAVE_TERMIO_H
#  include <termio.h>
# endif
#endif
#include <errno.h>
#ifdef HAVE_UNISTD_H
# include <unistd.h>
#endif
#include <stdio.h>
# include <string.h>
#ifdef HAVE_GRP_H
#include <grp.h>
#endif
#ifdef HAVE_PTY_H
#include <pty.h>
#endif
#ifdef HAVE_UTMP_H
#include <utmp.h>
#endif

#ifdef HAVE_PTSNAME
# include <stdlib.h>
# ifdef HAVE_STROPTS_H
#  include <stropts.h>
# endif
#endif

#ifdef HAVE_UTIL_H
#include <util.h>
#endif

#include "opal/util/opal_pty.h"

/* The only public interface is openpty - all others are to support
   openpty() */

#if OPAL_ENABLE_PTY_SUPPORT == 0

int opal_openpty(int *amaster, int *aslave, char *name,
                 void *termp, void *winpp)
{
    return -1;
}

#elif defined(HAVE_OPENPTY)

int opal_openpty(int *amaster, int *aslave, char *name,
                 struct termios *termp, struct winsize *winp)
{
    return openpty(amaster, aslave, name, termp, winp);
}

#else

/* implement openpty in terms of ptym_open and ptys_open */

static int ptym_open(char *pts_name);
static int ptys_open(int fdm, char *pts_name);

int opal_openpty(int *amaster, int *aslave, char *name,
                 struct termios *termp, struct winsize *winp)
{
    char line[20];
    *amaster = ptym_open(line);
    if (*amaster < 0) {
        return -1;
    }
    *aslave = ptys_open(*amaster, line);
    if (*aslave < 0) {
        close(*amaster);
        return -1;
    }
    if (name) {
        strcpy(name, line);
    }
#ifndef TCSAFLUSH
#define TCSAFLUSH TCSETAF
#endif
    if (termp) {
        (void) tcsetattr(*aslave, TCSAFLUSH, termp);
    }
#ifdef TIOCSWINSZ
    if (winp) {
        (void) ioctl(*aslave, TIOCSWINSZ, (char *) winp);
    }
#endif
    return 0;
}


static int ptym_open(char *pts_name)
{
    int fdm;
#ifdef HAVE_PTSNAME
    char *ptr;

#ifdef _AIX
    strcpy(pts_name, "/dev/ptc");
#else
    strcpy(pts_name, "/dev/ptmx");
#endif
    fdm = open(pts_name, O_RDWR);
    if (fdm < 0) {
        return -1;
    }
    if (grantpt(fdm) < 0) {     /* grant access to slave */
        close(fdm);
        return -2;
    }
    if (unlockpt(fdm) < 0) {    /* clear slave's lock flag */
        close(fdm);
        return -3;
    }
    ptr = ptsname(fdm);
    if (ptr == NULL) {          /* get slave's name */
        close(fdm);
        return -4;
    }
    strcpy(pts_name, ptr);      /* return name of slave */
    return fdm;                 /* return fd of master */
#else
    char *ptr1, *ptr2;

    strcpy(pts_name, "/dev/ptyXY");
    /* array index: 012345689 (for references in following code) */
    for (ptr1 = "pqrstuvwxyzPQRST"; *ptr1 != 0; ptr1++) {
        pts_name[8] = *ptr1;
        for (ptr2 = "0123456789abcdef"; *ptr2 != 0; ptr2++) {
            pts_name[9] = *ptr2;
            /* try to open master */
            fdm = open(pts_name, O_RDWR);
            if (fdm < 0) {
                if (errno == ENOENT) {  /* different from EIO */
                    return -1;  /* out of pty devices */
                } else {
                    continue;   /* try next pty device */
                }
            }
            pts_name[5] = 't';  /* chage "pty" to "tty" */
            return fdm;         /* got it, return fd of master */
        }
    }
    return -1;                  /* out of pty devices */
#endif
}


static int ptys_open(int fdm, char *pts_name)
{
    int fds;
#ifdef HAVE_PTSNAME
    /* following should allocate controlling terminal */
    fds = open(pts_name, O_RDWR);
    if (fds < 0) {
        close(fdm);
        return -5;
    }
#if defined(__SVR4) && defined(__sun)
    if (ioctl(fds, I_PUSH, "ptem") < 0) {
        close(fdm);
        close(fds);
        return -6;
    }
    if (ioctl(fds, I_PUSH, "ldterm") < 0) {
        close(fdm);
        close(fds);
        return -7;
    }
#endif

    return fds;
#else
    int gid;
    struct group *grptr;

    grptr = getgrnam("tty");
    if (grptr != NULL) {
        gid = grptr->gr_gid;
    } else {
        gid = -1;               /* group tty is not in the group file */
    }
    /* following two functions don't work unless we're root */
    chown(pts_name, getuid(), gid);
    chmod(pts_name, S_IRUSR | S_IWUSR | S_IWGRP);
    fds = open(pts_name, O_RDWR);
    if (fds < 0) {
        close(fdm);
        return -1;
    }
    return fds;
#endif
}

#endif /* #ifdef HAVE_OPENPTY */
