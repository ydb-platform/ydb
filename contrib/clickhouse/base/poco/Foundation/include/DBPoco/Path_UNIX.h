//
// Path_UNIX.h
//
// Library: Foundation
// Package: Filesystem
// Module:  Path
//
// Definition of the PathImpl class fo rUnix.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Foundation_Path_UNIX_INCLUDED
#define DB_Foundation_Path_UNIX_INCLUDED


#include <vector>
#include "DBPoco/Foundation.h"


namespace DBPoco
{


class PathImpl
{
public:
    static std::string currentImpl();
    static std::string homeImpl();
    static std::string configHomeImpl();
    static std::string dataHomeImpl();
    static std::string tempHomeImpl();
    static std::string cacheHomeImpl();
    static std::string tempImpl();
    static std::string configImpl();
    static std::string nullImpl();
    static std::string expandImpl(const std::string & path);
    static void listRootsImpl(std::vector<std::string> & roots);
};


} // namespace DBPoco


#endif // DB_Foundation_Path_UNIX_INCLUDED
