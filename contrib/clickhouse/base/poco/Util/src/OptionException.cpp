//
// OptionException.cpp
//
// Library: Util
// Package: Options
// Module:  OptionException
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Util/OptionException.h"
#include <typeinfo>


namespace DBPoco {
namespace Util {


DB_POCO_IMPLEMENT_EXCEPTION(OptionException, DBPoco::DataException, "Option exception")
DB_POCO_IMPLEMENT_EXCEPTION(UnknownOptionException, OptionException, "Unknown option specified")
DB_POCO_IMPLEMENT_EXCEPTION(AmbiguousOptionException, OptionException, "Ambiguous option specified")
DB_POCO_IMPLEMENT_EXCEPTION(MissingOptionException, OptionException, "Required option not specified")
DB_POCO_IMPLEMENT_EXCEPTION(MissingArgumentException, OptionException, "Missing option argument")
DB_POCO_IMPLEMENT_EXCEPTION(InvalidArgumentException, OptionException, "Invalid option argument")
DB_POCO_IMPLEMENT_EXCEPTION(UnexpectedArgumentException, OptionException, "Unexpected option argument")
DB_POCO_IMPLEMENT_EXCEPTION(IncompatibleOptionsException, OptionException, "Incompatible options")
DB_POCO_IMPLEMENT_EXCEPTION(DuplicateOptionException, OptionException, "Option must not be given more than once")
DB_POCO_IMPLEMENT_EXCEPTION(EmptyOptionException, OptionException, "Empty option specified")


} } // namespace DBPoco::Util
