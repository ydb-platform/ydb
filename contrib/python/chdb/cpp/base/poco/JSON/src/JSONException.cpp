//
// JSONException.cpp
//
// Library: JSON
// Package: JSON
// Module:  JSONException
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/JSON/JSONException.h"
#include <typeinfo>


namespace CHDBPoco {
namespace JSON {


CHDB_POCO_IMPLEMENT_EXCEPTION(JSONException, Exception, "JSON Exception")


} } // namespace CHDBPoco::JSON
