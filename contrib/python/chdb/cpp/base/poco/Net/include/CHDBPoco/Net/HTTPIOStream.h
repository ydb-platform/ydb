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


#ifndef CHDB_Net_HTTPIOStream_INCLUDED
#define CHDB_Net_HTTPIOStream_INCLUDED


#include "CHDBPoco/Net/HTTPResponse.h"
#include "CHDBPoco/Net/Net.h"
#include "CHDBPoco/UnbufferedStreamBuf.h"


namespace CHDBPoco
{
namespace Net
{


    class HTTPClientSession;


    class Net_API HTTPResponseStreamBuf : public CHDBPoco::UnbufferedStreamBuf
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
} // namespace CHDBPoco::Net


#endif // CHDB_Net_HTTPIOStream_INCLUDED
