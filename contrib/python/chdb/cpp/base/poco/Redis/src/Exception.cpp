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


#include "CHDBPoco/Redis/Exception.h"


namespace CHDBPoco {
namespace Redis {


CHDB_POCO_IMPLEMENT_EXCEPTION(RedisException, Exception, "Redis Exception")


} } // namespace CHDBPoco::Redis
