//
// LoggingSubsystem.cpp
//
// Library: Util
// Package: Application
// Module:  LoggingSubsystem
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Util/LoggingSubsystem.h"
#include "DBPoco/Util/LoggingConfigurator.h"
#include "DBPoco/Util/Application.h"
#include "DBPoco/Logger.h"


using DBPoco::Logger;


namespace DBPoco {
namespace Util {


LoggingSubsystem::LoggingSubsystem()
{
}


LoggingSubsystem::~LoggingSubsystem()
{
}


const char* LoggingSubsystem::name() const
{
	return "Logging Subsystem";
}

	
void LoggingSubsystem::initialize(Application& app)
{
	LoggingConfigurator configurator;
	configurator.configure(&app.config());
	std::string logger = app.config().getString("application.logger", "Application");
	app.setLogger(Logger::get(logger));
}


void LoggingSubsystem::uninitialize()
{
}


} } // namespace DBPoco::Util
