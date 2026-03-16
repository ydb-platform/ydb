/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/

#ifndef D4INCLUDES_H
#define D4INCLUDES_H 1

#include "config.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <inttypes.h>
#include <assert.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_STDINT_H
#include <stdint.h>
#endif

#include <curl/curl.h>

#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif

#ifdef HAVE_GETRLIMIT
#  ifdef HAVE_SYS_RESOURCE_H
#    include <sys/time.h>
#  endif
#  ifdef HAVE_SYS_RESOURCE_H
#    include <sys/resource.h>
#  endif
#endif

#include "netcdf.h"
#include "nc.h"
#include "ncrc.h"
#include "ncbytes.h"
#include "nclist.h"
#include "ncuri.h"
#include "nclog.h"
#include "ncdap.h"
#include "ncpathmgr.h"
#include "ncutil.h"

#include "d4util.h"

#include "ncd4types.h"
#include "ncd4.h"

#include "d4debug.h"
#include "d4chunk.h"

#endif /*D4INCLUDES_H*/

