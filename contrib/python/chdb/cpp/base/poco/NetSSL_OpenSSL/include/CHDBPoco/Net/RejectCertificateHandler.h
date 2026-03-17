//
// RejectCertificateHandler.h
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  RejectCertificateHandler
//
// Definition of the RejectCertificateHandler class.
//
// Copyright (c) 2006-2010, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_NetSSL_RejectCertificateHandler_INCLUDED
#define CHDB_NetSSL_RejectCertificateHandler_INCLUDED


#include "CHDBPoco/Net/InvalidCertificateHandler.h"
#include "CHDBPoco/Net/NetSSL.h"


namespace CHDBPoco
{
namespace Net
{


    class NetSSL_API RejectCertificateHandler : public InvalidCertificateHandler
    /// A RejectCertificateHandler is invoked whenever an error
    /// occurs verifying the certificate. It always rejects
    /// the certificate.
    {
    public:
        RejectCertificateHandler(bool handleErrorsOnServerSide);
        /// Creates the RejectCertificateHandler

        virtual ~RejectCertificateHandler();
        /// Destroys the RejectCertificateHandler.

        void onInvalidCertificate(const void * pSender, VerificationErrorArgs & errorCert);
    };


}
} // namespace CHDBPoco::Net


#endif // CHDB_NetSSL_RejectCertificateHandler_INCLUDED
