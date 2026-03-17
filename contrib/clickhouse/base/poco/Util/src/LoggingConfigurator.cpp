//
// LoggingConfigurator.cpp
//
// Library: Util
// Package: Configuration
// Module:  LoggingConfigurator
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Util/LoggingConfigurator.h"
#include "DBPoco/Util/AbstractConfiguration.h"
#include "DBPoco/AutoPtr.h"
#include "DBPoco/Channel.h"
#include "DBPoco/FormattingChannel.h"
#include "DBPoco/Formatter.h"
#include "DBPoco/PatternFormatter.h"
#include "DBPoco/Logger.h"
#include "DBPoco/LoggingRegistry.h"
#include "DBPoco/LoggingFactory.h"
#include <map>


using DBPoco::AutoPtr;
using DBPoco::Formatter;
using DBPoco::PatternFormatter;
using DBPoco::Channel;
using DBPoco::FormattingChannel;
using DBPoco::Logger;
using DBPoco::LoggingRegistry;
using DBPoco::LoggingFactory;


namespace DBPoco {
namespace Util {


LoggingConfigurator::LoggingConfigurator()
{
}


LoggingConfigurator::~LoggingConfigurator()
{
}


void LoggingConfigurator::configure(AbstractConfiguration* pConfig)
{
	DB_poco_check_ptr (pConfig);

	AutoPtr<AbstractConfiguration> pFormattersConfig(pConfig->createView("logging.formatters"));
	configureFormatters(pFormattersConfig);

	AutoPtr<AbstractConfiguration> pChannelsConfig(pConfig->createView("logging.channels"));
	configureChannels(pChannelsConfig);

	AutoPtr<AbstractConfiguration> pLoggersConfig(pConfig->createView("logging.loggers"));
	configureLoggers(pLoggersConfig);
}


void LoggingConfigurator::configureFormatters(AbstractConfiguration* pConfig)
{
	AbstractConfiguration::Keys formatters;
	pConfig->keys(formatters);
	for (AbstractConfiguration::Keys::const_iterator it = formatters.begin(); it != formatters.end(); ++it)
	{
		AutoPtr<AbstractConfiguration> pFormatterConfig(pConfig->createView(*it));
		AutoPtr<Formatter> pFormatter(createFormatter(pFormatterConfig));
		LoggingRegistry::defaultRegistry().registerFormatter(*it, pFormatter);
	}
}


void LoggingConfigurator::configureChannels(AbstractConfiguration* pConfig)
{
	AbstractConfiguration::Keys channels;
	pConfig->keys(channels);
	for (AbstractConfiguration::Keys::const_iterator it = channels.begin(); it != channels.end(); ++it)
	{
		AutoPtr<AbstractConfiguration> pChannelConfig(pConfig->createView(*it));
		AutoPtr<Channel> pChannel = createChannel(pChannelConfig);
		LoggingRegistry::defaultRegistry().registerChannel(*it, pChannel);
	}
	for (AbstractConfiguration::Keys::const_iterator it = channels.begin(); it != channels.end(); ++it)
	{
		AutoPtr<AbstractConfiguration> pChannelConfig(pConfig->createView(*it));
		Channel* pChannel = LoggingRegistry::defaultRegistry().channelForName(*it);
		configureChannel(pChannel, pChannelConfig);
	}
}


void LoggingConfigurator::configureLoggers(AbstractConfiguration* pConfig)
{
	typedef std::map<std::string, AutoPtr<AbstractConfiguration> > LoggerMap;

	AbstractConfiguration::Keys loggers;
	pConfig->keys(loggers);
	// use a map to sort loggers by their name, ensuring initialization in correct order (parents before children)
	LoggerMap loggerMap; 
	for (AbstractConfiguration::Keys::const_iterator it = loggers.begin(); it != loggers.end(); ++it)
	{
		AutoPtr<AbstractConfiguration> pLoggerConfig(pConfig->createView(*it));
		loggerMap[pLoggerConfig->getString("name", "")] = pLoggerConfig;
	}
	for (LoggerMap::iterator it = loggerMap.begin(); it != loggerMap.end(); ++it)
	{
		configureLogger(it->second);
	}
}


Formatter* LoggingConfigurator::createFormatter(AbstractConfiguration* pConfig)
{
	AutoPtr<Formatter> pFormatter(LoggingFactory::defaultFactory().createFormatter(pConfig->getString("class")));
	AbstractConfiguration::Keys props;
	pConfig->keys(props);
	for (AbstractConfiguration::Keys::const_iterator it = props.begin(); it != props.end(); ++it)
	{
		if (*it != "class")
			pFormatter->setProperty(*it, pConfig->getString(*it));		
	}
	return pFormatter.duplicate();
}


Channel* LoggingConfigurator::createChannel(AbstractConfiguration* pConfig)
{
	AutoPtr<Channel> pChannel(LoggingFactory::defaultFactory().createChannel(pConfig->getString("class")));
	AutoPtr<Channel> pWrapper(pChannel);
	AbstractConfiguration::Keys props;
	pConfig->keys(props);
	for (AbstractConfiguration::Keys::const_iterator it = props.begin(); it != props.end(); ++it)
	{
		if (*it == "pattern")
		{
			AutoPtr<Formatter> pPatternFormatter(new PatternFormatter(pConfig->getString(*it)));
			pWrapper = new FormattingChannel(pPatternFormatter, pChannel);
		}
		else if (*it == "formatter")
		{
			AutoPtr<FormattingChannel> pFormattingChannel(new FormattingChannel(0, pChannel));
			if (pConfig->hasProperty("formatter.class"))
			{
				AutoPtr<AbstractConfiguration> pFormatterConfig(pConfig->createView(*it));	
				AutoPtr<Formatter> pFormatter(createFormatter(pFormatterConfig));
				pFormattingChannel->setFormatter(pFormatter);
			}
			else pFormattingChannel->setProperty(*it, pConfig->getString(*it));
#if defined(__GNUC__) && __GNUC__ < 3
			pWrapper = pFormattingChannel.duplicate();
#else
			pWrapper = pFormattingChannel;
#endif
		}
	}
	return pWrapper.duplicate();
}


void LoggingConfigurator::configureChannel(Channel* pChannel, AbstractConfiguration* pConfig)
{
	AbstractConfiguration::Keys props;
	pConfig->keys(props);
	for (AbstractConfiguration::Keys::const_iterator it = props.begin(); it != props.end(); ++it)
	{
		if (*it != "pattern" && *it != "formatter" && *it != "class")
		{
			pChannel->setProperty(*it, pConfig->getString(*it));
		}
	}
}


void LoggingConfigurator::configureLogger(AbstractConfiguration* pConfig)
{
	Logger& logger = Logger::get(pConfig->getString("name", ""));
	AbstractConfiguration::Keys props;
	pConfig->keys(props);
	for (AbstractConfiguration::Keys::const_iterator it = props.begin(); it != props.end(); ++it)
	{
		if (*it == "channel" && pConfig->hasProperty("channel.class"))
		{
			AutoPtr<AbstractConfiguration> pChannelConfig(pConfig->createView(*it));	
			AutoPtr<Channel> pChannel(createChannel(pChannelConfig));
			configureChannel(pChannel, pChannelConfig);
			Logger::setChannel(logger.name(), pChannel);
		}
		else if (*it != "name")
		{
			Logger::setProperty(logger.name(), *it, pConfig->getString(*it));
		}
	}
}


} } // namespace DBPoco::Util
