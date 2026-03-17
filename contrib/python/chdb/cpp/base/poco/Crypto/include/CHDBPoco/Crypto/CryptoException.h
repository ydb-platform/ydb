//
// CryptoException.h
//
//
// Library: Crypto
// Package: Crypto
// Module:  CryptoException
//
// Definition of the CryptoException class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Crypto_CryptoException_INCLUDED
#define CHDB_Crypto_CryptoException_INCLUDED


#include "CHDBPoco/Crypto/Crypto.h"
#include "CHDBPoco/Exception.h"


namespace CHDBPoco
{
namespace Crypto
{


    CHDB_POCO_DECLARE_EXCEPTION(Crypto_API, CryptoException, CHDBPoco::Exception)


    class Crypto_API OpenSSLException : public CryptoException
    {
    public:
        OpenSSLException(int code = 0);
        OpenSSLException(const std::string & msg, int code = 0);
        OpenSSLException(const std::string & msg, const std::string & arg, int code = 0);
        OpenSSLException(const std::string & msg, const CHDBPoco::Exception & exc, int code = 0);
        OpenSSLException(const OpenSSLException & exc);
        ~OpenSSLException() throw();
        OpenSSLException & operator=(const OpenSSLException & exc);
        const char * name() const throw();
        const char * className() const throw();
        CHDBPoco::Exception * clone() const;
        void rethrow() const;

    private:
        void setExtMessage();
    };


}
} // namespace CHDBPoco::Crypto


#endif // CHDB_Crypto_CryptoException_INCLUDED
