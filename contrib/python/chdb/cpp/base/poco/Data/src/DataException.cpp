//
// DataException.cpp
//
// Library: Data
// Package: DataCore
// Module:  DataException
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Data/DataException.h"
#include <typeinfo>


namespace CHDBPoco {
namespace Data {


CHDB_POCO_IMPLEMENT_EXCEPTION(DataException, CHDBPoco::IOException, "Database Exception")
CHDB_POCO_IMPLEMENT_EXCEPTION(RowDataMissingException, DataException, "Data for row missing")
CHDB_POCO_IMPLEMENT_EXCEPTION(UnknownDataBaseException, DataException, "Type of data base unknown")
CHDB_POCO_IMPLEMENT_EXCEPTION(UnknownTypeException, DataException, "Type of data unknown")
CHDB_POCO_IMPLEMENT_EXCEPTION(ExecutionException, DataException, "Execution error")
CHDB_POCO_IMPLEMENT_EXCEPTION(BindingException, DataException, "Binding error")
CHDB_POCO_IMPLEMENT_EXCEPTION(ExtractException, DataException, "Extraction error")
CHDB_POCO_IMPLEMENT_EXCEPTION(LimitException, DataException, "Limit error")
CHDB_POCO_IMPLEMENT_EXCEPTION(NotSupportedException, DataException, "Feature or property not supported")
CHDB_POCO_IMPLEMENT_EXCEPTION(SessionUnavailableException, DataException, "Session is unavailable")
CHDB_POCO_IMPLEMENT_EXCEPTION(SessionPoolExhaustedException, DataException, "No more sessions available from the session pool")
CHDB_POCO_IMPLEMENT_EXCEPTION(SessionPoolExistsException, DataException, "Session already exists in the pool")
CHDB_POCO_IMPLEMENT_EXCEPTION(NoDataException, DataException, "No data found")
CHDB_POCO_IMPLEMENT_EXCEPTION(LengthExceededException, DataException, "Data too long")
CHDB_POCO_IMPLEMENT_EXCEPTION(ConnectionFailedException, DataException, "Connection attempt failed")
CHDB_POCO_IMPLEMENT_EXCEPTION(NotConnectedException, DataException, "Not connected to data source")


} } // namespace CHDBPoco::Data
