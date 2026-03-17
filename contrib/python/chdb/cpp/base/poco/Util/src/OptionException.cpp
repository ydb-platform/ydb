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


#include "CHDBPoco/Util/OptionException.h"
#include <typeinfo>


namespace CHDBPoco {
namespace Util {


CHDB_POCO_IMPLEMENT_EXCEPTION(OptionException, CHDBPoco::DataException, "Option exception")
CHDB_POCO_IMPLEMENT_EXCEPTION(UnknownOptionException, OptionException, "Unknown option specified")
CHDB_POCO_IMPLEMENT_EXCEPTION(AmbiguousOptionException, OptionException, "Ambiguous option specified")
CHDB_POCO_IMPLEMENT_EXCEPTION(MissingOptionException, OptionException, "Required option not specified")
CHDB_POCO_IMPLEMENT_EXCEPTION(MissingArgumentException, OptionException, "Missing option argument")
CHDB_POCO_IMPLEMENT_EXCEPTION(InvalidArgumentException, OptionException, "Invalid option argument")
CHDB_POCO_IMPLEMENT_EXCEPTION(UnexpectedArgumentException, OptionException, "Unexpected option argument")
CHDB_POCO_IMPLEMENT_EXCEPTION(IncompatibleOptionsException, OptionException, "Incompatible options")
CHDB_POCO_IMPLEMENT_EXCEPTION(DuplicateOptionException, OptionException, "Option must not be given more than once")
CHDB_POCO_IMPLEMENT_EXCEPTION(EmptyOptionException, OptionException, "Empty option specified")


} } // namespace CHDBPoco::Util
