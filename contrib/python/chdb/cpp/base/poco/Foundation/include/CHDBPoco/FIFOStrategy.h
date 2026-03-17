//
// FIFOStrategy.h
//
// Library: Foundation
// Package: Events
// Module:  FIFOStragegy
//
// Implementation of the FIFOStrategy template.
//
// Copyright (c) 2006-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Foundation_FIFOStrategy_INCLUDED
#define CHDB_Foundation_FIFOStrategy_INCLUDED


#include "CHDBPoco/DefaultStrategy.h"


namespace CHDBPoco
{


//@ deprecated
template <class TArgs, class TDelegate>
class FIFOStrategy : public DefaultStrategy<TArgs, TDelegate>
/// Note: As of release 1.4.2, DefaultStrategy already
/// implements FIFO behavior, so this class is provided
/// for backwards compatibility only.
{
public:
    FIFOStrategy() { }

    FIFOStrategy(const FIFOStrategy & s) : DefaultStrategy<TArgs, TDelegate>(s) { }

    ~FIFOStrategy() { }

    FIFOStrategy & operator=(const FIFOStrategy & s)
    {
        DefaultStrategy<TArgs, TDelegate>::operator=(s);
        return *this;
    }
};


} // namespace CHDBPoco


#endif // CHDB_Foundation_FIFOStrategy_INCLUDED
