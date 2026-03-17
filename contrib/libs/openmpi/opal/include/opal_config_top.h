/*
 * Copyright (c) 2011 Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 * This file is included at the top of opal_config.h, and is
 * therefore a) before all the #define's that were output from
 * configure, and b) included in most/all files in Open MPI.
 *
 * Since this file is *only* ever included by opal_config.h, and
 * opal_config.h already has #ifndef/#endif protection, there is no
 * need to #ifndef/#endif protection here.
 */

#ifndef OPAL_CONFIG_H
#error "opal_config_top.h should only be included from opal_config.h"
#endif

/* The only purpose of this file is to undef the PACKAGE_<foo> macros
   that are put in by autoconf/automake projects.  Specifically, if
   you include a .h file from another project that defines these
   macros (e.g., gmp.h) and then include OMPI/ORTE/OPAL's config.h,
   you'll get a preprocessor conflict.  So put these undef's here to
   protect us from other package's PACKAGE_<foo> macros.

   Note that we can't put them directly in opal_config.h (e.g., via
   AH_TOP) because they will be turned into #define's by autoconf. */

#undef PACKAGE_BUGREPORT
#undef PACKAGE_NAME
#undef PACKAGE_STRING
#undef PACKAGE_TARNAME
#undef PACKAGE_VERSION
#undef PACKAGE_URL
#undef HAVE_CONFIG_H
