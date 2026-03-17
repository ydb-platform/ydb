//
// PipeImpl.h
//
// Library: Foundation
// Package: Processes
// Module:  PipeImpl
//
// Definition of the PipeImpl class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Foundation_PipeImpl_INCLUDED
#define DB_Foundation_PipeImpl_INCLUDED


#include "DBPoco/Foundation.h"


#if   defined(DB_POCO_OS_FAMILY_UNIX)
#    include "DBPoco/PipeImpl_POSIX.h"
#else
#    error #include "DBPoco/PipeImpl_DUMMY.h"
#endif


#endif // DB_Foundation_PipeImpl_INCLUDED
