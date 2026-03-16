//
// PipeImpl_POSIX.h
//
// Library: Foundation
// Package: Processes
// Module:  PipeImpl
//
// Definition of the PipeImpl class for POSIX.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Foundation_PipeImpl_POSIX_INCLUDED
#define DB_Foundation_PipeImpl_POSIX_INCLUDED


#include "DBPoco/Foundation.h"
#include "DBPoco/RefCountedObject.h"


namespace DBPoco
{


class Foundation_API PipeImpl : public RefCountedObject
/// A dummy implementation of PipeImpl for platforms
/// that do not support pipes.
{
public:
    typedef int Handle;

    PipeImpl();
    ~PipeImpl();
    int writeBytes(const void * buffer, int length);
    int readBytes(void * buffer, int length);
    Handle readHandle() const;
    Handle writeHandle() const;
    void closeRead();
    void closeWrite();

private:
    int _readfd;
    int _writefd;
};


} // namespace DBPoco


#endif // DB_Foundation_PipeImpl_POSIX_INCLUDED
