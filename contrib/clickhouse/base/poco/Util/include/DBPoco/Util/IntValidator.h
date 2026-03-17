//
// IntValidator.h
//
// Library: Util
// Package: Options
// Module:  IntValidator
//
// Definition of the IntValidator class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Util_IntValidator_INCLUDED
#define DB_Util_IntValidator_INCLUDED


#include "DBPoco/Util/Util.h"
#include "DBPoco/Util/Validator.h"


namespace DBPoco
{
namespace Util
{


    class Util_API IntValidator : public Validator
    /// The IntValidator tests whether the option argument,
    /// which must be an integer, lies within a given range.
    {
    public:
        IntValidator(int min, int max);
        /// Creates the IntValidator.

        ~IntValidator();
        /// Destroys the IntValidator.

        void validate(const Option & option, const std::string & value);
        /// Validates the value for the given option by
        /// testing whether it's an integer that lies within
        /// a given range.

    private:
        IntValidator();

        int _min;
        int _max;
    };


}
} // namespace DBPoco::Util


#endif // DB_Util_IntValidator_INCLUDED
