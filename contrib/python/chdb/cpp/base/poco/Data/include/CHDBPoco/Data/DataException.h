//
// DataException.h
//
// Library: Data
// Package: DataCore
// Module:  DataException
//
// Definition of the DataException class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Data_DataException_INCLUDED
#define CHDB_Data_DataException_INCLUDED


#include "CHDBPoco/Data/Data.h"
#include "CHDBPoco/Exception.h"


namespace CHDBPoco
{
namespace Data
{


    CHDB_POCO_DECLARE_EXCEPTION(Data_API, DataException, CHDBPoco::IOException)
    CHDB_POCO_DECLARE_EXCEPTION(Data_API, RowDataMissingException, DataException)
    CHDB_POCO_DECLARE_EXCEPTION(Data_API, UnknownDataBaseException, DataException)
    CHDB_POCO_DECLARE_EXCEPTION(Data_API, UnknownTypeException, DataException)
    CHDB_POCO_DECLARE_EXCEPTION(Data_API, ExecutionException, DataException)
    CHDB_POCO_DECLARE_EXCEPTION(Data_API, BindingException, DataException)
    CHDB_POCO_DECLARE_EXCEPTION(Data_API, ExtractException, DataException)
    CHDB_POCO_DECLARE_EXCEPTION(Data_API, LimitException, DataException)
    CHDB_POCO_DECLARE_EXCEPTION(Data_API, NotSupportedException, DataException)
    CHDB_POCO_DECLARE_EXCEPTION(Data_API, SessionUnavailableException, DataException)
    CHDB_POCO_DECLARE_EXCEPTION(Data_API, SessionPoolExhaustedException, DataException)
    CHDB_POCO_DECLARE_EXCEPTION(Data_API, SessionPoolExistsException, DataException)
    CHDB_POCO_DECLARE_EXCEPTION(Data_API, NoDataException, DataException)
    CHDB_POCO_DECLARE_EXCEPTION(Data_API, LengthExceededException, DataException)
    CHDB_POCO_DECLARE_EXCEPTION(Data_API, ConnectionFailedException, DataException)
    CHDB_POCO_DECLARE_EXCEPTION(Data_API, NotConnectedException, DataException)


}
} // namespace CHDBPoco::Data


#endif // CHDB_Data_DataException_INCLUDED
