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


#ifndef DB_Net_HTTPStreamFactory_INCLUDED
#define DB_Net_HTTPStreamFactory_INCLUDED


#include "DBPoco/Net/HTTPSession.h"
#include "DBPoco/Net/Net.h"
#include "DBPoco/URIStreamFactory.h"


namespace DBPoco
{
namespace Net
{


    class Net_API HTTPStreamFactory : public DBPoco::URIStreamFactory
    /// An implementation of the URIStreamFactory interface
    /// that handles Hyper-Text Transfer Protocol (http) URIs.
    {
    public:
        HTTPStreamFactory();
        /// Creates the HTTPStreamFactory.

        HTTPStreamFactory(const std::string & proxyHost, DBPoco::UInt16 proxyPort = HTTPSession::HTTP_PORT);
        /// Creates the HTTPStreamFactory.
        ///
        /// HTTP connections will use the given proxy.

        HTTPStreamFactory(
            const std::string & proxyHost, DBPoco::UInt16 proxyPort, const std::string & proxyUsername, const std::string & proxyPassword);
        /// Creates the HTTPStreamFactory.
        ///
        /// HTTP connections will use the given proxy and
        /// will be authorized against the proxy using Basic authentication
        /// with the given proxyUsername and proxyPassword.

        virtual ~HTTPStreamFactory();
        /// Destroys the HTTPStreamFactory.

        virtual std::istream * open(const DBPoco::URI & uri);
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
        DBPoco::UInt16 _proxyPort;
        std::string _proxyUsername;
        std::string _proxyPassword;
    };


}
} // namespace DBPoco::Net


#endif // DB_Net_HTTPStreamFactory_INCLUDED
