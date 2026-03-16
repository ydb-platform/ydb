//
// expat_config.h
//
// Poco XML specific configuration for expat.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef EXPAT_CONFIG_H
#define EXPAT_CONFIG_H


#include "DBPoco/Platform.h"


#    include <memory.h>
#include <string.h>


#define XML_CONTEXT_BYTES 1024


#if defined DB_POCO_ARCH_LITTLE_ENDIAN
#    define BYTEORDER 1234
#else
#    define BYTEORDER 4321
#endif


#define HAVE_MEMMOVE


#endif /* EXPAT_CONFIG_H */
