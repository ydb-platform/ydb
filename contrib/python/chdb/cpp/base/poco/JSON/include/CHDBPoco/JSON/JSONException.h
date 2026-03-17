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


#ifndef CHDB_JSON_JSONException_INCLUDED
#define CHDB_JSON_JSONException_INCLUDED


#include "CHDBPoco/Exception.h"
#include "CHDBPoco/JSON/JSON.h"


namespace CHDBPoco
{
namespace JSON
{


    CHDB_POCO_DECLARE_EXCEPTION(JSON_API, JSONException, CHDBPoco::Exception)


}
} // namespace CHDBPoco::JSON


#endif // CHDB_JSON_JSONException_INCLUDED
