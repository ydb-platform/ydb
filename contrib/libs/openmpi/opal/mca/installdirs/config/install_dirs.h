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
 * Copyright (c) 2007      Los Alamos National Security, LLC.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 * This file should be included by any file that needs the
 * installation directories hard-coded into the object file.  This
 * should be avoided if at all possible, but there are some places
 * (like the wrapper compilers) where it is infinitely easier to have
 * the paths stored.
 *
 * If you have questions about which directory to use, we try as best
 * we can to follow the GNU coding standards on this issue.  The
 * description of each directory can be found at the following URL:
 *
 * http://www.gnu.org/prep/standards/html_node/Directory-Variables.html
 *
 * The line below is to shut AC 2.60 up about datarootdir.  Don't remove.
 * datarootdir=foo
 */

#ifndef OPAL_INST_DIRS_H
#define OPAL_INST_DIRS_H

#define OPAL_PREFIX "/var/empty/openmpi-4.0.1"
#define OPAL_EXEC_PREFIX "${prefix}"

/* The directory for installing executable programs that users can
   run. */
#define OPAL_BINDIR "${exec_prefix}/bin"

/* The directory for installing executable programs that can be run
   from the shell, but are only generally useful to system
   administrators. */
#define OPAL_SBINDIR "${exec_prefix}/sbin"

/* The directory for installing executable programs to be run by other
   programs rather than by users.

   The definition of ‘libexecdir’ is the same for all packages, so
   you should install your data in a subdirectory thereof. Most
   packages install their data under $(libexecdir)/package-name/,
   possibly within additional subdirectories thereof, such as
   $(libexecdir)/package-name/machine/version. */
#define OPAL_LIBEXECDIR "${exec_prefix}/libexec"

/* The root of the directory tree for read-only
   architecture-independent data files.

   See not about OPAL_DATADIR.  And you probably want that one, not
   this one.  This is one of those "building block" paths, that is
   really only used for defining other paths. */
#define OPAL_DATAROOTDIR "${prefix}/share"

/* The directory for installing idiosyncratic read-only
  architecture-independent data files for this program.

  The definition of ‘datadir’ is the same for all packages, so you
  should install your data in a subdirectory thereof. Most packages
  install their data under $(datadir)/package-name/. */
#define OPAL_DATADIR "${datarootdir}"

/* $(datadir)/package-name/.  You probably want to use this instead of
   OPAL_DATADIR */
#define OPAL_PKGDATADIR "${datadir}/openmpi"

/* The directory for installing read-only data files that pertain to a
   single machine–that is to say, files for configuring a host. Mailer
   and network configuration files, /etc/passwd, and so forth belong
   here. All the files in this directory should be ordinary ASCII text
   files.

   Do not install executables here in this directory (they probably
   belong in $(libexecdir) or $(sbindir)). Also do not install files
   that are modified in the normal course of their use (programs whose
   purpose is to change the configuration of the system
   excluded). Those probably belong in $(localstatedir).  */
#define OPAL_SYSCONFDIR "${prefix}/etc"

/* The directory for installing architecture-independent data files
   which the programs modify while they run. */
#define OPAL_SHAREDSTATEDIR "${prefix}/com"

/* The directory for installing data files which the programs modify
   while they run, and that pertain to one specific machine. Users
   should never need to modify files in this directory to configure
   the package's operation; put such configuration information in
   separate files that go in $(datadir) or
   $(sysconfdir). */
#define OPAL_LOCALSTATEDIR "${prefix}/var"

/* The directory for object files and libraries of object code. Do not
   install executables here, they probably ought to go in
   $(libexecdir) instead. */
#define OPAL_LIBDIR "${exec_prefix}/lib"

/* $(libdir)/package-name/.  This is where components should go */
#define OPAL_PKGLIBDIR "${libdir}/openmpi"

/* The directory for installing header files to be included by user
   programs with the C ‘#include’ preprocessor directive. */
#define OPAL_INCLUDEDIR "${prefix}/include"

/* $(includedir)/package-name/.  The devel headers go in here */
#define OPAL_PKGINCLUDEDIR "${includedir}/openmpi"

/* The directory for installing the Info files for this package. */
#define OPAL_INFODIR "${datarootdir}/info"

/* The top-level directory for installing the man pages (if any) for
   this package. */
#define OPAL_MANDIR "${datarootdir}/man"

#endif
