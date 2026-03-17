//
// ServerApplication.cpp
//
// Library: Util
// Package: Application
// Module:  ServerApplication
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Util/ServerApplication.h"
#include "DBPoco/Util/Option.h"
#include "DBPoco/Util/OptionSet.h"
#include "DBPoco/Util/OptionException.h"
#include "DBPoco/FileStream.h"
#include "DBPoco/Exception.h"
#include "DBPoco/Process.h"
#include "DBPoco/NamedEvent.h"
#include "DBPoco/NumberFormatter.h"
#include "DBPoco/Logger.h"
#include "DBPoco/String.h"
#if defined(DB_POCO_OS_FAMILY_UNIX) && !defined(POCO_VXWORKS)
#include "DBPoco/TemporaryFile.h"
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <signal.h>
#include <sys/stat.h>
#include <fstream>
#endif


using DBPoco::NumberFormatter;
using DBPoco::Exception;
using DBPoco::SystemException;


namespace DBPoco {
namespace Util {


#if defined(POCO_VXWORKS) || DB_POCO_OS == DB_POCO_OS_ANDROID
DBPoco::Event ServerApplication::_terminate;
#endif


ServerApplication::ServerApplication()
{
}


ServerApplication::~ServerApplication()
{
}


bool ServerApplication::isInteractive() const
{
	bool runsInBackground = config().getBool("application.runAsDaemon", false) || config().getBool("application.runAsService", false);
	return !runsInBackground;
}


int ServerApplication::run()
{
	return Application::run();
}


void ServerApplication::terminate()
{
#if   defined(POCO_VXWORKS) || DB_POCO_OS == DB_POCO_OS_ANDROID
	_terminate.set();
#else
	DBPoco::Process::requestTermination(Process::id());
#endif
}


#if   defined(DB_POCO_OS_FAMILY_UNIX)


//
// Unix specific code
//
void ServerApplication::waitForTerminationRequest()
{
#if DB_POCO_OS != DB_POCO_OS_ANDROID
	sigset_t sset;
	sigemptyset(&sset);
	if (!std::getenv("POCO_ENABLE_DEBUGGER"))
	{
		sigaddset(&sset, SIGINT);
	}
	sigaddset(&sset, SIGQUIT);
	sigaddset(&sset, SIGTERM);
	sigprocmask(SIG_BLOCK, &sset, NULL);
	int sig;
	sigwait(&sset, &sig);
#else // DB_POCO_OS != DB_POCO_OS_ANDROID
	_terminate.wait();
#endif
}


int ServerApplication::run(int argc, char** argv)
{
	bool runAsDaemon = isDaemon(argc, argv);
	if (runAsDaemon)
	{
		beDaemon();
	}
	try
	{
		init(argc, argv);
		if (runAsDaemon)
		{
			int rc = chdir("/");
			if (rc != 0) return EXIT_OSERR;
		}
	}
	catch (Exception& exc)
	{
		logger().log(exc);
		return EXIT_CONFIG;
	}
	return run();
}


int ServerApplication::run(const std::vector<std::string>& args)
{
	bool runAsDaemon = false;
	for (std::vector<std::string>::const_iterator it = args.begin(); it != args.end(); ++it)
	{
		if (*it == "--daemon")
		{
			runAsDaemon = true;
			break;
		}
	}
	if (runAsDaemon)
	{
		beDaemon();
	}
	try
	{
		init(args);
		if (runAsDaemon)
		{
			int rc = chdir("/");
			if (rc != 0) return EXIT_OSERR;
		}
	}
	catch (Exception& exc)
	{
		logger().log(exc);
		return EXIT_CONFIG;
	}
	return run();
}


bool ServerApplication::isDaemon(int argc, char** argv)
{
	std::string option("--daemon");
	for (int i = 1; i < argc; ++i)
	{
		if (option == argv[i])
			return true;
	}
	return false;
}


void ServerApplication::beDaemon()
{
#if !defined(POCO_NO_FORK_EXEC)
	pid_t pid;
	if ((pid = fork()) < 0)
		throw SystemException("cannot fork daemon process");
	else if (pid != 0)
		exit(0);

	setsid();
	umask(027);

	// attach stdin, stdout, stderr to /dev/null
	// instead of just closing them. This avoids
	// issues with third party/legacy code writing
	// stuff to stdout/stderr.
	FILE* fin  = freopen("/dev/null", "r+", stdin);
	if (!fin) throw DBPoco::OpenFileException("Cannot attach stdin to /dev/null");
	FILE* fout = freopen("/dev/null", "r+", stdout);
	if (!fout) throw DBPoco::OpenFileException("Cannot attach stdout to /dev/null");
	FILE* ferr = freopen("/dev/null", "r+", stderr);
	if (!ferr) throw DBPoco::OpenFileException("Cannot attach stderr to /dev/null");
#else
	throw DBPoco::NotImplementedException("platform does not allow fork/exec");
#endif
}


void ServerApplication::defineOptions(OptionSet& options)
{
	Application::defineOptions(options);

	options.addOption(
		Option("daemon", "", "Run application as a daemon.")
			.required(false)
			.repeatable(false)
			.callback(OptionCallback<ServerApplication>(this, &ServerApplication::handleDaemon)));

	options.addOption(
		Option("umask", "", "Set the daemon's umask (octal, e.g. 027).")
			.required(false)
			.repeatable(false)
			.argument("mask")
			.callback(OptionCallback<ServerApplication>(this, &ServerApplication::handleUMask)));

	options.addOption(
		Option("pidfile", "", "Write the process ID of the application to given file.")
			.required(false)
			.repeatable(false)
			.argument("path")
			.callback(OptionCallback<ServerApplication>(this, &ServerApplication::handlePidFile)));
}


void ServerApplication::handleDaemon(const std::string& name, const std::string& value)
{
	config().setBool("application.runAsDaemon", true);
}


void ServerApplication::handleUMask(const std::string& name, const std::string& value)
{
	int mask = 0;
	for (std::string::const_iterator it = value.begin(); it != value.end(); ++it)
	{
		mask *= 8;
		if (*it >= '0' && *it <= '7')
			mask += *it - '0';
		else
			throw DBPoco::InvalidArgumentException("umask contains non-octal characters", value);
	}
	umask(mask);
}


void ServerApplication::handlePidFile(const std::string& name, const std::string& value)
{
	DBPoco::FileOutputStream ostr(value);
	if (ostr.good())
		ostr << DBPoco::Process::id() << std::endl;
	else
		throw DBPoco::CreateFileException("Cannot write PID to file", value);
	DBPoco::TemporaryFile::registerForDeletion(value);
}


#endif


} } // namespace DBPoco::Util
