//
// Exception.h
//
// Library: Redis
// Package: Redis
// Module:  Exception
//
// Implementation of the Exception class.
//
// Copyright (c) 2015, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Redis/Exception.h"


namespace DBPoco {
namespace Redis {


DB_POCO_IMPLEMENT_EXCEPTION(RedisException, Exception, "Redis Exception")


} } // namespace DBPoco::Redis
