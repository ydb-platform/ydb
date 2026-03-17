//
// OptionException.h
//
// Library: Util
// Package: Options
// Module:  OptionException
//
// Definition of the OptionException class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Util_OptionException_INCLUDED
#define CHDB_Util_OptionException_INCLUDED


#include "CHDBPoco/Exception.h"
#include "CHDBPoco/Util/Util.h"


namespace CHDBPoco
{
namespace Util
{


    CHDB_POCO_DECLARE_EXCEPTION(Util_API, OptionException, CHDBPoco::DataException)
    CHDB_POCO_DECLARE_EXCEPTION(Util_API, UnknownOptionException, OptionException)
    CHDB_POCO_DECLARE_EXCEPTION(Util_API, AmbiguousOptionException, OptionException)
    CHDB_POCO_DECLARE_EXCEPTION(Util_API, MissingOptionException, OptionException)
    CHDB_POCO_DECLARE_EXCEPTION(Util_API, MissingArgumentException, OptionException)
    CHDB_POCO_DECLARE_EXCEPTION(Util_API, InvalidArgumentException, OptionException)
    CHDB_POCO_DECLARE_EXCEPTION(Util_API, UnexpectedArgumentException, OptionException)
    CHDB_POCO_DECLARE_EXCEPTION(Util_API, IncompatibleOptionsException, OptionException)
    CHDB_POCO_DECLARE_EXCEPTION(Util_API, DuplicateOptionException, OptionException)
    CHDB_POCO_DECLARE_EXCEPTION(Util_API, EmptyOptionException, OptionException)


}
} // namespace CHDBPoco::Util


#endif // CHDB_Util_OptionException_INCLUDED
