//
// Instantiator.h
//
// Library: Foundation
// Package: Core
// Module:  Instantiator
//
// Definition of the Instantiator class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Foundation_Instantiator_INCLUDED
#define DB_Foundation_Instantiator_INCLUDED


#include "DBPoco/Foundation.h"


namespace DBPoco
{


template <class Base>
class AbstractInstantiator
/// The common base class for all Instantiator instantiations.
/// Used by DynamicFactory.
{
public:
    AbstractInstantiator()
    /// Creates the AbstractInstantiator.
    {
    }

    virtual ~AbstractInstantiator()
    /// Destroys the AbstractInstantiator.
    {
    }

    virtual Base * createInstance() const = 0;
    /// Creates an instance of a concrete subclass of Base.

private:
    AbstractInstantiator(const AbstractInstantiator &);
    AbstractInstantiator & operator=(const AbstractInstantiator &);
};


template <class C, class Base>
class Instantiator : public AbstractInstantiator<Base>
/// A template class for the easy instantiation of
/// instantiators.
///
/// For the Instantiator to work, the class of which
/// instances are to be instantiated must have a no-argument
/// constructor.
{
public:
    Instantiator()
    /// Creates the Instantiator.
    {
    }

    virtual ~Instantiator()
    /// Destroys the Instantiator.
    {
    }

    Base * createInstance() const { return new C; }
};


} // namespace DBPoco


#endif // DB_Foundation_Instantiator_INCLUDED
