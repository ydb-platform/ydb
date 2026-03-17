//
// CompressedLogFile.h
//
// Library: Foundation
// Package: Logging
// Module:  CompressedLogFile
//
// Definition of the LogFile class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Foundation_CompressedLogFile_INCLUDED
#define DB_Foundation_CompressedLogFile_INCLUDED


#include "DBPoco/Buffer.h"
#include "DBPoco/Foundation.h"
#include "DBPoco/LogFile.h"

#include <lz4.h>
#include <lz4frame.h>


namespace DBPoco
{


class Foundation_API CompressedLogFile : public LogFile
{
public:
    CompressedLogFile(const std::string & path);
    /// Allocates buffer and initializes compession state.

    ~CompressedLogFile();
    /// Destoys CompressedLogFile

    void write(const std::string & text, bool flush = true);
    /// Writes the given text to the compressed log file.
    /// If flush is true, the text will be immediately
    /// flushed to the file.

private:
    DBPoco::Buffer<char> _buffer;

    LZ4F_preferences_t _kPrefs;
    LZ4F_compressionContext_t _ctx;
};


} // namespace DBPoco


#endif // DB_Foundation_CompressedLogFile_INCLUDED
