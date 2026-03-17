//
// Limit.h
//
// Library: Data
// Package: DataCore
// Module:  Limit
//
// Definition of the Limit class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Data_Limit_INCLUDED
#define CHDB_Data_Limit_INCLUDED


#include "CHDBPoco/Data/Data.h"


namespace CHDBPoco
{
namespace Data
{


    class Data_API Limit
    /// Limit stores information how many rows a query should return.
    {
    public:
        typedef CHDBPoco::UInt32 SizeT;

        enum Type
        {
            LIMIT_UNLIMITED = ~((SizeT)0)
        };

        Limit(SizeT value, bool hardLimit = false, bool isLowerLimit = false);
        /// Creates the Limit.
        ///
        /// Value contains the upper row hint, if hardLimit is set to true, the limit acts as a hard
        /// border, ie. every query must return exactly value rows, returning more than value objects will throw an exception!
        /// LowerLimits always act as hard-limits!
        ///
        /// A value of LIMIT_UNLIMITED disables the limit.

        ~Limit();
        /// Destroys the Limit.

        SizeT value() const;
        /// Returns the value of the limit

        bool isHardLimit() const;
        /// Returns true if the limit is a hard limit.

        bool isLowerLimit() const;
        /// Returns true if the limit is a lower limit, otherwise it is an upperLimit

        bool operator==(const Limit & other) const;
        /// Equality operator.

        bool operator!=(const Limit & other) const;
        /// Inequality operator.

    private:
        SizeT _value;
        bool _hardLimit;
        bool _isLowerLimit;
    };


    //
    // inlines
    //
    inline CHDBPoco::UInt32 Limit::value() const
    {
        return _value;
    }


    inline bool Limit::isHardLimit() const
    {
        return _hardLimit;
    }


    inline bool Limit::isLowerLimit() const
    {
        return _isLowerLimit;
    }


    inline bool Limit::operator==(const Limit & other) const
    {
        return other._value == _value && other._hardLimit == _hardLimit && other._isLowerLimit == _isLowerLimit;
    }


    inline bool Limit::operator!=(const Limit & other) const
    {
        return other._value != _value || other._hardLimit != _hardLimit || other._isLowerLimit != _isLowerLimit;
    }


}
} // namespace CHDBPoco::Data


#endif // CHDB_Data_Limit_INCLUDED
