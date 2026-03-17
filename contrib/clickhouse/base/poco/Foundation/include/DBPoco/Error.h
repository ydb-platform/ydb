//
// Error.h
//
// Library: Foundation
// Package: Core
// Module:  Error
//
// Definition of the Error class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Foundation_Error_INCLUDED
#define DB_Foundation_Error_INCLUDED


#include "DBPoco/Foundation.h"


namespace DBPoco
{


class Foundation_API Error
/// The Error class provides utility functions
/// for error reporting.
{
public:
    static int last();
    /// Utility function returning the last error.

    static std::string getMessage(int errorCode);
    /// Utility function translating numeric error code to string.
};


} // namespace DBPoco


#endif // DB_Foundation_Error_INCLUDED
