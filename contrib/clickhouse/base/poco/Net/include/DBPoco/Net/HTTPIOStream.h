//
// HTTPIOStream.h
//
// Library: Net
// Package: HTTP
// Module:  HTTPIOStream
//
// Definition of the HTTPIOStream class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Net_HTTPIOStream_INCLUDED
#define DB_Net_HTTPIOStream_INCLUDED


#include "DBPoco/Net/HTTPResponse.h"
#include "DBPoco/Net/Net.h"
#include "DBPoco/UnbufferedStreamBuf.h"


namespace DBPoco
{
namespace Net
{


    class HTTPClientSession;


    class Net_API HTTPResponseStreamBuf : public DBPoco::UnbufferedStreamBuf
    {
    public:
        HTTPResponseStreamBuf(std::istream & istr);

        ~HTTPResponseStreamBuf();

    private:
        int readFromDevice();

        std::istream & _istr;
    };


    inline int HTTPResponseStreamBuf::readFromDevice()
    {
        return _istr.get();
    }


    class Net_API HTTPResponseIOS : public virtual std::ios
    {
    public:
        HTTPResponseIOS(std::istream & istr);

        ~HTTPResponseIOS();

        HTTPResponseStreamBuf * rdbuf();

    protected:
        HTTPResponseStreamBuf _buf;
    };


    inline HTTPResponseStreamBuf * HTTPResponseIOS::rdbuf()
    {
        return &_buf;
    }


    class Net_API HTTPResponseStream : public HTTPResponseIOS, public std::istream
    {
    public:
        HTTPResponseStream(std::istream & istr, HTTPClientSession * pSession);

        ~HTTPResponseStream();

    private:
        HTTPClientSession * _pSession;
    };


}
} // namespace DBPoco::Net


#endif // DB_Net_HTTPIOStream_INCLUDED
