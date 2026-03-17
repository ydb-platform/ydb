//
// StatementCreator.h
//
// Library: Data
// Package: DataCore
// Module:  StatementCreator
//
// Definition of the StatementCreator class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Data_StatementCreator_INCLUDED
#define CHDB_Data_StatementCreator_INCLUDED


#include "CHDBPoco/AutoPtr.h"
#include "CHDBPoco/Data/Data.h"
#include "CHDBPoco/Data/SessionImpl.h"
#include "CHDBPoco/Data/Statement.h"


namespace CHDBPoco
{
namespace Data
{


    class Data_API StatementCreator
    /// A StatementCreator creates Statements.
    {
    public:
        StatementCreator();
        /// Creates an uninitialized StatementCreator.

        StatementCreator(CHDBPoco::AutoPtr<SessionImpl> ptrImpl);
        /// Creates a StatementCreator.

        StatementCreator(const StatementCreator & other);
        /// Creates a StatementCreator by copying another one.

        ~StatementCreator();
        /// Destroys the StatementCreator.

        StatementCreator & operator=(const StatementCreator & other);
        /// Assignment operator.

        void swap(StatementCreator & other);
        /// Swaps the StatementCreator with another one.

        template <typename T>
        Statement operator<<(const T & t)
        /// Creates a Statement.
        {
            if (!_ptrImpl->isConnected())
                throw NotConnectedException(_ptrImpl->connectionString());

            Statement stmt(_ptrImpl->createStatementImpl());
            stmt << t;
            return stmt;
        }

    private:
        CHDBPoco::AutoPtr<SessionImpl> _ptrImpl;
    };


}
} // namespace CHDBPoco::Data


#endif // CHDB_Data_StatementCreator_INCLUDED
