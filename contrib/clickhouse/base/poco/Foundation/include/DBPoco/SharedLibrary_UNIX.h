//
// SharedLibrary_UNIX.h
//
// Library: Foundation
// Package: SharedLibrary
// Module:  SharedLibrary
//
// Definition of the SharedLibraryImpl class for UNIX (dlopen).
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Foundation_SharedLibrary_UNIX_INCLUDED
#define DB_Foundation_SharedLibrary_UNIX_INCLUDED


#include "DBPoco/Foundation.h"
#include "DBPoco/Mutex.h"


namespace DBPoco
{


class Foundation_API SharedLibraryImpl
{
protected:
    enum Flags
    {
        SHLIB_GLOBAL_IMPL = 1,
        SHLIB_LOCAL_IMPL = 2
    };

    SharedLibraryImpl();
    ~SharedLibraryImpl();
    void loadImpl(const std::string & path, int flags);
    void unloadImpl();
    bool isLoadedImpl() const;
    void * findSymbolImpl(const std::string & name);
    const std::string & getPathImpl() const;
    static std::string suffixImpl();
    static bool setSearchPathImpl(const std::string & path);

private:
    std::string _path;
    void * _handle;
    static FastMutex _mutex;
};


} // namespace DBPoco


#endif // DB_Foundation_SharedLibrary_UNIX_INCLUDED
