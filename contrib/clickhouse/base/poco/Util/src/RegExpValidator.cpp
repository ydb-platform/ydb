//
// RegExpValidator.cpp
//
// Library: Util
// Package: Options
// Module:  RegExpValidator
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Util/RegExpValidator.h"
#include "DBPoco/Util/Option.h"
#include "DBPoco/Util/OptionException.h"
#include "DBPoco/RegularExpression.h"
#include "DBPoco/Format.h"


using DBPoco::format;


namespace DBPoco {
namespace Util {


RegExpValidator::RegExpValidator(const std::string& regexp):
	_regexp(regexp)
{
}


RegExpValidator::~RegExpValidator()
{
}


void RegExpValidator::validate(const Option& option, const std::string& value)
{
	if (!RegularExpression::match(value, _regexp, RegularExpression::RE_ANCHORED | RegularExpression::RE_UTF8))
		throw InvalidArgumentException(format("argument for %s does not match regular expression %s", option.fullName(), _regexp));
}


} } // namespace DBPoco::Util
