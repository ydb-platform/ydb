//
// Connector.h
//
// Library: Data
// Package: DataCore
// Module:  Connector
//
// Definition of the Connector class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Data_Connector_INCLUDED
#define CHDB_Data_Connector_INCLUDED


#include "CHDBPoco/AutoPtr.h"
#include "CHDBPoco/Data/Data.h"
#include "CHDBPoco/Data/SessionImpl.h"


namespace CHDBPoco
{
namespace Data
{


    class Data_API Connector
    /// A Connector creates SessionImpl objects.
    ///
    /// Every connector library (like the SQLite or the ODBC connector)
    /// provides a subclass of this class, an instance of which is
    /// registered with the SessionFactory.
    {
    public:
        Connector();
        /// Creates the Connector.

        virtual ~Connector();
        /// Destroys the Connector.

        virtual const std::string & name() const = 0;
        /// Returns the name associated with this connector.

        virtual CHDBPoco::AutoPtr<SessionImpl>
        createSession(const std::string & connectionString, std::size_t timeout = SessionImpl::LOGIN_TIMEOUT_DEFAULT) = 0;
        /// Create a SessionImpl object and initialize it with the given connectionString.
    };


}
} // namespace CHDBPoco::Data


#endif // CHDB_Data_Connector_INCLUDED
