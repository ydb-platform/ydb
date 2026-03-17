//
// IntValidator.cpp
//
// Library: Util
// Package: Options
// Module:  IntValidator
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Util/IntValidator.h"
#include "DBPoco/Util/Option.h"
#include "DBPoco/Util/OptionException.h"
#include "DBPoco/NumberParser.h"
#include "DBPoco/Format.h"


using DBPoco::NumberParser;
using DBPoco::format;


namespace DBPoco {
namespace Util {


IntValidator::IntValidator(int min, int max):
	_min(min),
	_max(max)
{
}


IntValidator::~IntValidator()
{
}


void IntValidator::validate(const Option& option, const std::string& value)
{
	int n;
	if (NumberParser::tryParse(value, n))
	{
		if (n < _min || n > _max)
			throw InvalidArgumentException(format("argument for %s must be in range %d to %d", option.fullName(), _min, _max));
	}
	else throw InvalidArgumentException(format("argument for %s must be an integer", option.fullName()));
}


} } // namespace DBPoco::Util
