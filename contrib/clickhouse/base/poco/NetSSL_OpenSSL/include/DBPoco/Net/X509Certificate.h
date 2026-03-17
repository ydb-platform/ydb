//
// X509Certificate.h
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  X509Certificate
//
// Definition of the X509Certificate class.
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_NetSSL_X509Certificate_INCLUDED
#define DB_NetSSL_X509Certificate_INCLUDED


#include <set>
#include "DBPoco/Crypto/X509Certificate.h"
#include "DBPoco/DateTime.h"
#include "DBPoco/Net/NetSSL.h"
#include "DBPoco/Net/SocketDefs.h"
#include "DBPoco/SharedPtr.h"


namespace DBPoco
{
namespace Net
{


    class HostEntry;


    class NetSSL_API X509Certificate : public DBPoco::Crypto::X509Certificate
    /// This class extends DBPoco::Crypto::X509Certificate with the
    /// feature to validate a certificate.
    {
    public:
        explicit X509Certificate(std::istream & istr);
        /// Creates the X509Certificate object by reading
        /// a certificate in PEM format from a stream.

        explicit X509Certificate(const std::string & path);
        /// Creates the X509Certificate object by reading
        /// a certificate in PEM format from a file.

        explicit X509Certificate(X509 * pCert);
        /// Creates the X509Certificate from an existing
        /// OpenSSL certificate. Ownership is taken of
        /// the certificate.

        X509Certificate(X509 * pCert, bool shared);
        /// Creates the X509Certificate from an existing
        /// OpenSSL certificate. Ownership is taken of
        /// the certificate. If shared is true, the
        /// certificate's reference count is incremented.

        X509Certificate(const DBPoco::Crypto::X509Certificate & cert);
        /// Creates the certificate by copying another one.

        X509Certificate & operator=(const DBPoco::Crypto::X509Certificate & cert);
        /// Assigns a certificate.

        ~X509Certificate();
        /// Destroys the X509Certificate.

        bool verify(const std::string & hostName) const;
        /// Verifies the validity of the certificate against the host name.
        ///
        /// For this check to be successful, the certificate must contain
        /// a domain name that matches the domain name
        /// of the host.
        ///
        /// Returns true if verification succeeded, or false otherwise.

        static bool verify(const DBPoco::Crypto::X509Certificate & cert, const std::string & hostName);
        /// Verifies the validity of the certificate against the host name.
        ///
        /// For this check to be successful, the certificate must contain
        /// a domain name that matches the domain name
        /// of the host.
        ///
        /// Returns true if verification succeeded, or false otherwise.

    protected:
        static bool containsWildcards(const std::string & commonName);
        static bool matchWildcard(const std::string & alias, const std::string & hostName);

    private:
        enum
        {
            NAME_BUFFER_SIZE = 256
        };
    };


}
} // namespace DBPoco::Net


#endif // DB_NetSSL_X509Certificate_INCLUDED
