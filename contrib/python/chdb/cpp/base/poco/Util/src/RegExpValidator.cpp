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


#include "CHDBPoco/Util/RegExpValidator.h"
#include "CHDBPoco/Util/Option.h"
#include "CHDBPoco/Util/OptionException.h"
#include "CHDBPoco/RegularExpression.h"
#include "CHDBPoco/Format.h"


using CHDBPoco::format;


namespace CHDBPoco {
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


} } // namespace CHDBPoco::Util
