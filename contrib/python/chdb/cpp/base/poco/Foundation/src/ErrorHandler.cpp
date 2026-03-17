//
// ErrorHandler.cpp
//
// Library: Foundation
// Package: Threading
// Module:  ErrorHandler
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:    BSL-1.0
//


#include "CHDBPoco/ErrorHandler.h"
#include "CHDBPoco/SingletonHolder.h"


namespace CHDBPoco {


ErrorHandler* ErrorHandler::_pHandler = ErrorHandler::defaultHandler();
FastMutex ErrorHandler::_mutex;


ErrorHandler::ErrorHandler()
{
}


ErrorHandler::~ErrorHandler()
{
}


void ErrorHandler::exception(const Exception& exc)
{
    CHDB_poco_debugger_msg(exc.what());
}


void ErrorHandler::exception(const std::exception& exc)
{
    CHDB_poco_debugger_msg(exc.what());
}


void ErrorHandler::exception()
{
    CHDB_poco_debugger_msg("unknown exception");
}


void ErrorHandler::handle(const Exception& exc)
{
    FastMutex::ScopedLock lock(_mutex);
    try
    {
        _pHandler->exception(exc);
    }
    catch (...)
    {
    }
}


void ErrorHandler::handle(const std::exception& exc)
{
    FastMutex::ScopedLock lock(_mutex);
    try
    {
        _pHandler->exception(exc);
    }
    catch (...)
    {
    }
}


void ErrorHandler::handle()
{
    FastMutex::ScopedLock lock(_mutex);
    try
    {
        _pHandler->exception();
    }
    catch (...)
    {
    }
}

void ErrorHandler::logMessage(Message::Priority priority, const std::string & msg)
{
    FastMutex::ScopedLock lock(_mutex);
    try
    {
        _pHandler->logMessageImpl(priority, msg);
    }
    catch (...)
    {
    }
}


ErrorHandler* ErrorHandler::set(ErrorHandler* pHandler)
{
    CHDB_poco_check_ptr(pHandler);

    FastMutex::ScopedLock lock(_mutex);
    ErrorHandler* pOld = _pHandler;
    _pHandler = pHandler;
    return pOld;
}


ErrorHandler* ErrorHandler::defaultHandler()
{
    // NOTE: Since this is called to initialize the static _pHandler
    // variable, sh has to be a local static, otherwise we run
    // into static initialization order issues.
    static SingletonHolder<ErrorHandler> sh;
    return sh.get();
}


} // namespace CHDBPoco
