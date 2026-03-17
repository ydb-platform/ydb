//
// Validator.h
//
// Library: Util
// Package: Options
// Module:  Validator
//
// Definition of the Validator class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Util_Validator_INCLUDED
#define DB_Util_Validator_INCLUDED


#include "DBPoco/RefCountedObject.h"
#include "DBPoco/Util/Util.h"


namespace DBPoco
{
namespace Util
{


    class Option;


    class Util_API Validator : public DBPoco::RefCountedObject
    /// Validator specifies the interface for option validators.
    ///
    /// Option validators provide a simple way for the automatic
    /// validation of command line argument values.
    {
    public:
        virtual void validate(const Option & option, const std::string & value) = 0;
        /// Validates the value for the given option.
        /// Does nothing if the value is valid.
        ///
        /// Throws an OptionException otherwise.

    protected:
        Validator();
        /// Creates the Validator.

        virtual ~Validator();
        /// Destroys the Validator.
    };


}
} // namespace DBPoco::Util


#endif // DB_Util_Validator_INCLUDED
