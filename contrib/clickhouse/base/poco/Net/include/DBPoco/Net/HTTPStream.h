//
// HTTPStream.h
//
// Library: Net
// Package: HTTP
// Module:  HTTPStream
//
// Definition of the HTTPStream class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Net_HTTPStream_INCLUDED
#define DB_Net_HTTPStream_INCLUDED


#include <cstddef>
#include <istream>
#include <ostream>
#include "DBPoco/Net/HTTPBasicStreamBuf.h"
#include "DBPoco/Net/Net.h"


namespace DBPoco
{
namespace Net
{


    class HTTPSession;


    class Net_API HTTPStreamBuf : public HTTPBasicStreamBuf
    /// This is the streambuf class used for reading and writing
    /// HTTP message bodies.
    {
    public:
        typedef HTTPBasicStreamBuf::openmode openmode;

        HTTPStreamBuf(HTTPSession & session, openmode mode);
        ~HTTPStreamBuf();
        void close();

    protected:
        int readFromDevice(char * buffer, std::streamsize length);
        int writeToDevice(const char * buffer, std::streamsize length);

    private:
        HTTPSession & _session;
        openmode _mode;
    };


    class Net_API HTTPIOS : public virtual std::ios
    /// The base class for HTTPInputStream.
    {
    public:
        HTTPIOS(HTTPSession & session, HTTPStreamBuf::openmode mode);
        ~HTTPIOS();
        HTTPStreamBuf * rdbuf();

        bool isComplete() const { return false; }

    protected:
        HTTPStreamBuf _buf;
    };


    class Net_API HTTPInputStream : public HTTPIOS, public std::istream
    /// This class is for internal use by HTTPSession only.
    {
    public:
        HTTPInputStream(HTTPSession & session);
        ~HTTPInputStream();
    };


    class Net_API HTTPOutputStream : public HTTPIOS, public std::ostream
    /// This class is for internal use by HTTPSession only.
    {
    public:
        HTTPOutputStream(HTTPSession & session);
        ~HTTPOutputStream();
    };


}
} // namespace DBPoco::Net


#endif // DB_Net_HTTPStream_INCLUDED
