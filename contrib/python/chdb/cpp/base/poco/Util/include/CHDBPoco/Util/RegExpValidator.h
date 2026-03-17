//
// RegExpValidator.h
//
// Library: Util
// Package: Options
// Module:  RegExpValidator
//
// Definition of the RegExpValidator class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Util_RegExpValidator_INCLUDED
#define CHDB_Util_RegExpValidator_INCLUDED


#include "CHDBPoco/Util/Util.h"
#include "CHDBPoco/Util/Validator.h"


namespace CHDBPoco
{
namespace Util
{


    class Util_API RegExpValidator : public Validator
    /// This validator matches the option value against
    /// a regular expression.
    {
    public:
        RegExpValidator(const std::string & regexp);
        /// Creates the RegExpValidator, using the given regular expression.

        ~RegExpValidator();
        /// Destroys the RegExpValidator.

        void validate(const Option & option, const std::string & value);
        /// Validates the value for the given option by
        /// matching it with the regular expression.

    private:
        RegExpValidator();

        std::string _regexp;
    };


}
} // namespace CHDBPoco::Util


#endif // CHDB_Util_RegExpValidator_INCLUDED
