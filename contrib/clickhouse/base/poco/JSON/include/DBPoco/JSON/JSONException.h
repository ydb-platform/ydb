//
// JSONException.h
//
// Library: JSON
// Package: JSON
// Module:  JSONException
//
// Definition of the JSONException class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_JSON_JSONException_INCLUDED
#define DB_JSON_JSONException_INCLUDED


#include "DBPoco/Exception.h"
#include "DBPoco/JSON/JSON.h"


namespace DBPoco
{
namespace JSON
{


    DB_POCO_DECLARE_EXCEPTION(JSON_API, JSONException, DBPoco::Exception)


}
} // namespace DBPoco::JSON


#endif // DB_JSON_JSONException_INCLUDED
