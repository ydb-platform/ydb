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


#ifndef CHDB_Foundation_PipeImpl_INCLUDED
#define CHDB_Foundation_PipeImpl_INCLUDED


#include "CHDBPoco/Foundation.h"


#if   defined(CHDB_POCO_OS_FAMILY_UNIX)
#    include "CHDBPoco/PipeImpl_POSIX.h"
#else
#    error #include "CHDBPoco/PipeImpl_DUMMY.h"
#endif


#endif // CHDB_Foundation_PipeImpl_INCLUDED
