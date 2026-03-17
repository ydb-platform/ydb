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


#ifndef DB_Util_OptionException_INCLUDED
#define DB_Util_OptionException_INCLUDED


#include "DBPoco/Exception.h"
#include "DBPoco/Util/Util.h"


namespace DBPoco
{
namespace Util
{


    DB_POCO_DECLARE_EXCEPTION(Util_API, OptionException, DBPoco::DataException)
    DB_POCO_DECLARE_EXCEPTION(Util_API, UnknownOptionException, OptionException)
    DB_POCO_DECLARE_EXCEPTION(Util_API, AmbiguousOptionException, OptionException)
    DB_POCO_DECLARE_EXCEPTION(Util_API, MissingOptionException, OptionException)
    DB_POCO_DECLARE_EXCEPTION(Util_API, MissingArgumentException, OptionException)
    DB_POCO_DECLARE_EXCEPTION(Util_API, InvalidArgumentException, OptionException)
    DB_POCO_DECLARE_EXCEPTION(Util_API, UnexpectedArgumentException, OptionException)
    DB_POCO_DECLARE_EXCEPTION(Util_API, IncompatibleOptionsException, OptionException)
    DB_POCO_DECLARE_EXCEPTION(Util_API, DuplicateOptionException, OptionException)
    DB_POCO_DECLARE_EXCEPTION(Util_API, EmptyOptionException, OptionException)


}
} // namespace DBPoco::Util


#endif // DB_Util_OptionException_INCLUDED
