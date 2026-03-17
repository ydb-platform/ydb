//
// Exception.h
//
// Library: Redis
// Package: Redis
// Module:  Exception
//
// Definition of the Exception class.
//
// Copyright (c) 2015, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Redis_Exception_INCLUDED
#define DB_Redis_Exception_INCLUDED


#include <typeinfo>
#include "DBPoco/Exception.h"
#include "DBPoco/Redis/Redis.h"


namespace DBPoco
{
namespace Redis
{


    DB_POCO_DECLARE_EXCEPTION(Redis_API, RedisException, Exception)


}
} // namespace DBPoco::Redis


#endif // DB_Redis_Exception_INCLUDED
