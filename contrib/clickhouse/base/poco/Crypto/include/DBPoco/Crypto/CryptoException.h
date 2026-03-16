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


#ifndef DB_Crypto_CryptoException_INCLUDED
#define DB_Crypto_CryptoException_INCLUDED


#include "DBPoco/Crypto/Crypto.h"
#include "DBPoco/Exception.h"


namespace DBPoco
{
namespace Crypto
{


    DB_POCO_DECLARE_EXCEPTION(Crypto_API, CryptoException, DBPoco::Exception)


    class Crypto_API OpenSSLException : public CryptoException
    {
    public:
        OpenSSLException(int code = 0);
        OpenSSLException(const std::string & msg, int code = 0);
        OpenSSLException(const std::string & msg, const std::string & arg, int code = 0);
        OpenSSLException(const std::string & msg, const DBPoco::Exception & exc, int code = 0);
        OpenSSLException(const OpenSSLException & exc);
        ~OpenSSLException() throw();
        OpenSSLException & operator=(const OpenSSLException & exc);
        const char * name() const throw();
        const char * className() const throw();
        DBPoco::Exception * clone() const;
        void rethrow() const;

    private:
        void setExtMessage();
    };


}
} // namespace DBPoco::Crypto


#endif // DB_Crypto_CryptoException_INCLUDED
