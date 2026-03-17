//
// Runnable.h
//
// Library: Foundation
// Package: Threading
// Module:  Thread
//
// Definition of the Runnable class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Foundation_Runnable_INCLUDED
#define DB_Foundation_Runnable_INCLUDED


#include "DBPoco/Foundation.h"


namespace DBPoco
{


class Foundation_API Runnable
/// The Runnable interface with the run() method
/// must be implemented by classes that provide
/// an entry point for a thread.
{
public:
    Runnable();
    virtual ~Runnable();

    virtual void run() = 0;
    /// Do whatever the thread needs to do. Must
    /// be overridden by subclasses.
};


} // namespace DBPoco


#endif // DB_Foundation_Runnable_INCLUDED
