//
// LoggingSubsystem.h
//
// Library: Util
// Package: Application
// Module:  LoggingSubsystem
//
// Definition of the LoggingSubsystem class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Util_LoggingSubsystem_INCLUDED
#define CHDB_Util_LoggingSubsystem_INCLUDED


#include "CHDBPoco/Util/Subsystem.h"
#include "CHDBPoco/Util/Util.h"


namespace CHDBPoco
{
namespace Util
{


    class Util_API LoggingSubsystem : public Subsystem
    /// The LoggingSubsystem class initializes the logging
    /// framework using the LoggingConfigurator.
    ///
    /// It also sets the Application's logger to
    /// the logger specified by the "application.logger"
    /// property, or to "Application" if the property
    /// is not specified.
    {
    public:
        LoggingSubsystem();
        const char * name() const;

    protected:
        void initialize(Application & self);
        void uninitialize();
        ~LoggingSubsystem();
    };


}
} // namespace CHDBPoco::Util


#endif // CHDB_Util_LoggingSubsystem_INCLUDED
