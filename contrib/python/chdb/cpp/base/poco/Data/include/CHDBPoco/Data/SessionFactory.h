//
// SessionFactory.h
//
// Library: Data
// Package: DataCore
// Module:  SessionFactory
//
// Definition of the SessionFactory class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Data_SessionFactory_INCLUDED
#define CHDB_Data_SessionFactory_INCLUDED


#include <map>
#include "CHDBPoco/Data/Connector.h"
#include "CHDBPoco/Data/Data.h"
#include "CHDBPoco/Data/Session.h"
#include "CHDBPoco/Mutex.h"
#include "CHDBPoco/SharedPtr.h"
#include "CHDBPoco/String.h"


namespace CHDBPoco
{
namespace Data
{


    class Data_API SessionFactory
    /// A SessionFactory is a singleton class that stores Connectors and allows to
    /// create Sessions of the required type:
    ///
    ///     Session ses(SessionFactory::instance().create(connector, connectionString));
    ///
    /// where the first param presents the type of session one wants to create (e.g. for SQLite one would choose "SQLite")
    /// and the second param is the connection string that the connector requires to connect to the database.
    ///
    /// A concrete example to open an SQLite database stored in the file "dummy.db" would be
    ///
    ///     Session ses(SessionFactory::instance().create(SQLite::Connector::KEY, "dummy.db"));
    ///
    /// An even simpler way to create a session is to use the two argument constructor of Session, which
    /// automatically invokes the SessionFactory:
    ///
    ///      Session ses("SQLite", "dummy.db");
    {
    public:
        static SessionFactory & instance();
        /// returns the static instance of the singleton.

        void add(Connector * pIn);
        /// Registers a Connector under its key at the factory. If a registration for that
        /// key is already active, the first registration will be kept, only its reference count will be increased.
        /// Always takes ownership of parameter pIn.

        void remove(const std::string & key);
        /// Lowers the reference count for the Connector registered under that key. If the count reaches zero,
        /// the object is removed.

        Session create(const std::string & key, const std::string & connectionString, std::size_t timeout = Session::LOGIN_TIMEOUT_DEFAULT);
        /// Creates a Session for the given key with the connectionString. Throws an Poco:Data::UnknownDataBaseException
        /// if no Connector is registered for that key.

        Session create(const std::string & uri, std::size_t timeout = Session::LOGIN_TIMEOUT_DEFAULT);
        /// Creates a Session for the given URI (must be in key:///connectionString format).
        /// Throws a Poco:Data::UnknownDataBaseException if no Connector is registered for the key.

    private:
        SessionFactory();
        ~SessionFactory();
        SessionFactory(const SessionFactory &);
        SessionFactory & operator=(const SessionFactory &);

        struct SessionInfo
        {
            int cnt;
            CHDBPoco::SharedPtr<Connector> ptrSI;
            SessionInfo(Connector * pSI);
        };

        typedef std::map<std::string, SessionInfo, CHDBPoco::CILess> Connectors;
        Connectors _connectors;
        CHDBPoco::FastMutex _mutex;
    };


}
} // namespace CHDBPoco::Data


#endif // CHDB_Data_SessionFactory_INCLUDED
