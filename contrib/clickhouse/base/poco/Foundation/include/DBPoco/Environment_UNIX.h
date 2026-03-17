//
// Environment_UNIX.h
//
// Library: Foundation
// Package: Core
// Module:  Environment
//
// Definition of the EnvironmentImpl class for Unix.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Foundation_Environment_UNIX_INCLUDED
#define DB_Foundation_Environment_UNIX_INCLUDED


#include <map>
#include "DBPoco/Foundation.h"
#include "DBPoco/Mutex.h"


namespace DBPoco
{


class Foundation_API EnvironmentImpl
{
public:
    typedef UInt8 NodeId[6]; /// Ethernet address.

    static std::string getImpl(const std::string & name);
    static bool hasImpl(const std::string & name);
    static void setImpl(const std::string & name, const std::string & value);
    static std::string osNameImpl();
    static std::string osDisplayNameImpl();
    static std::string osVersionImpl();
    static std::string osArchitectureImpl();
    static std::string nodeNameImpl();
    static void nodeIdImpl(NodeId & id);
    static unsigned processorCountImpl();

private:
    typedef std::map<std::string, std::string> StringMap;

    static StringMap _map;
    static FastMutex _mutex;
};


} // namespace DBPoco


#endif // DB_Foundation_Environment_UNIX_INCLUDED
