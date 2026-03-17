//
// HTTPStreamFactory.h
//
// Library: Net
// Package: HTTP
// Module:  HTTPStreamFactory
//
// Definition of the HTTPStreamFactory class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Net_HTTPStreamFactory_INCLUDED
#define CHDB_Net_HTTPStreamFactory_INCLUDED


#include "CHDBPoco/Net/HTTPSession.h"
#include "CHDBPoco/Net/Net.h"
#include "CHDBPoco/URIStreamFactory.h"


namespace CHDBPoco
{
namespace Net
{


    class Net_API HTTPStreamFactory : public CHDBPoco::URIStreamFactory
    /// An implementation of the URIStreamFactory interface
    /// that handles Hyper-Text Transfer Protocol (http) URIs.
    {
    public:
        HTTPStreamFactory();
        /// Creates the HTTPStreamFactory.

        HTTPStreamFactory(const std::string & proxyHost, CHDBPoco::UInt16 proxyPort = HTTPSession::HTTP_PORT);
        /// Creates the HTTPStreamFactory.
        ///
        /// HTTP connections will use the given proxy.

        HTTPStreamFactory(
            const std::string & proxyHost, CHDBPoco::UInt16 proxyPort, const std::string & proxyUsername, const std::string & proxyPassword);
        /// Creates the HTTPStreamFactory.
        ///
        /// HTTP connections will use the given proxy and
        /// will be authorized against the proxy using Basic authentication
        /// with the given proxyUsername and proxyPassword.

        virtual ~HTTPStreamFactory();
        /// Destroys the HTTPStreamFactory.

        virtual std::istream * open(const CHDBPoco::URI & uri);
        /// Creates and opens a HTTP stream for the given URI.
        /// The URI must be a http://... URI.
        ///
        /// Throws a NetException if anything goes wrong.
        ///
        /// Redirect responses are handled and the redirect
        /// location is automatically resolved, as long
        /// as the redirect location is still accessible
        /// via the HTTP protocol. If a redirection to
        /// a non http://... URI is received, a
        /// UnsupportedRedirectException exception is thrown.
        /// The offending URI can then be obtained via the message()
        /// method of UnsupportedRedirectException.

        static void registerFactory();
        /// Registers the HTTPStreamFactory with the
        /// default URIStreamOpener instance.

        static void unregisterFactory();
        /// Unregisters the HTTPStreamFactory with the
        /// default URIStreamOpener instance.

    private:
        enum
        {
            MAX_REDIRECTS = 10
        };

        std::string _proxyHost;
        CHDBPoco::UInt16 _proxyPort;
        std::string _proxyUsername;
        std::string _proxyPassword;
    };


}
} // namespace CHDBPoco::Net


#endif // CHDB_Net_HTTPStreamFactory_INCLUDED
