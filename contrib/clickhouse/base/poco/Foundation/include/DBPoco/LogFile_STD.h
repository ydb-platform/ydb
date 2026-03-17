//
// LogFile_STD.h
//
// Library: Foundation
// Package: Logging
// Module:  LogFile
//
// Definition of the LogFileImpl class using iostreams.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Foundation_LogFile_STD_INCLUDED
#define DB_Foundation_LogFile_STD_INCLUDED


#include "DBPoco/FileStream.h"
#include "DBPoco/Foundation.h"
#include "DBPoco/Timestamp.h"


namespace DBPoco
{


class Foundation_API LogFileImpl
/// The implementation of LogFile for non-Windows platforms.
/// The native filesystem APIs are used for
/// total control over locking behavior.
{
public:
    LogFileImpl(const std::string & path);
    ~LogFileImpl();
    void writeImpl(const std::string & text, bool flush);
    void writeBinaryImpl(const char * data, size_t size, bool flush);
    UInt64 sizeImpl() const;
    Timestamp creationDateImpl() const;
    const std::string & pathImpl() const;

private:
    std::string _path;
    mutable DBPoco::FileOutputStream _str;
    Timestamp _creationDate;
};


} // namespace DBPoco


#endif // DB_Foundation_LogFile_STD_INCLUDED
