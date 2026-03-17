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


#ifndef DB_Util_LoggingSubsystem_INCLUDED
#define DB_Util_LoggingSubsystem_INCLUDED


#include "DBPoco/Util/Subsystem.h"
#include "DBPoco/Util/Util.h"


namespace DBPoco
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
} // namespace DBPoco::Util


#endif // DB_Util_LoggingSubsystem_INCLUDED
