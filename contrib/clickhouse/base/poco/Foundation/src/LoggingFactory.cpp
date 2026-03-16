//
// LoggingFactory.cpp
//
// Library: Foundation
// Package: Logging
// Module:  LoggingFactory
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/LoggingFactory.h"
#include "DBPoco/SingletonHolder.h"
#include "DBPoco/AsyncChannel.h"
#include "DBPoco/ConsoleChannel.h"
#include "DBPoco/FileChannel.h"
#include "DBPoco/FormattingChannel.h"
#include "DBPoco/SplitterChannel.h"
#include "DBPoco/NullChannel.h"
#include "DBPoco/EventChannel.h"
#if defined(DB_POCO_OS_FAMILY_UNIX) && !defined(DB_POCO_NO_SYSLOGCHANNEL)
#include "DBPoco/SyslogChannel.h"
#endif
#include "DBPoco/PatternFormatter.h"


namespace DBPoco {


LoggingFactory::LoggingFactory()
{
	registerBuiltins();
}


LoggingFactory::~LoggingFactory()
{
}


void LoggingFactory::registerChannelClass(const std::string& className, ChannelInstantiator* pFactory)
{
	_channelFactory.registerClass(className, pFactory);
}


void LoggingFactory::registerFormatterClass(const std::string& className, FormatterFactory* pFactory)
{
	_formatterFactory.registerClass(className, pFactory);
}


Channel* LoggingFactory::createChannel(const std::string& className) const
{
	return _channelFactory.createInstance(className);
}


Formatter* LoggingFactory::createFormatter(const std::string& className) const
{
	return _formatterFactory.createInstance(className);
}


namespace
{
	static SingletonHolder<LoggingFactory> sh;
}


LoggingFactory& LoggingFactory::defaultFactory()
{
	return *sh.get();
}


void LoggingFactory::registerBuiltins()
{
	_channelFactory.registerClass("AsyncChannel", new Instantiator<AsyncChannel, Channel>);
	_channelFactory.registerClass("ConsoleChannel", new Instantiator<ConsoleChannel, Channel>);
	_channelFactory.registerClass("ColorConsoleChannel", new Instantiator<ColorConsoleChannel, Channel>);
#ifndef DB_POCO_NO_FILECHANNEL
	_channelFactory.registerClass("FileChannel", new Instantiator<FileChannel, Channel>);
#endif
	_channelFactory.registerClass("FormattingChannel", new Instantiator<FormattingChannel, Channel>);
#ifndef DB_POCO_NO_SPLITTERCHANNEL
	_channelFactory.registerClass("SplitterChannel", new Instantiator<SplitterChannel, Channel>);
#endif
	_channelFactory.registerClass("NullChannel", new Instantiator<NullChannel, Channel>);
	_channelFactory.registerClass("EventChannel", new Instantiator<EventChannel, Channel>);

#if defined(DB_POCO_OS_FAMILY_UNIX)
#ifndef DB_POCO_NO_SYSLOGCHANNEL
	_channelFactory.registerClass("SyslogChannel", new Instantiator<SyslogChannel, Channel>);
#endif
#endif

	_formatterFactory.registerClass("PatternFormatter", new Instantiator<PatternFormatter, Formatter>);
}


} // namespace DBPoco
