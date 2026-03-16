//
// PipeImpl.cpp
//
// Library: Foundation
// Package: Processes
// Module:  PipeImpl
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/PipeImpl.h"


#if   defined(DB_POCO_OS_FAMILY_UNIX)
#include "PipeImpl_POSIX.cpp"
#else
#error #include "PipeImpl_DUMMY.cpp"
#endif
