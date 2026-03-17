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


#ifndef CHDB_Redis_Exception_INCLUDED
#define CHDB_Redis_Exception_INCLUDED


#include <typeinfo>
#include "CHDBPoco/Exception.h"
#include "CHDBPoco/Redis/Redis.h"


namespace CHDBPoco
{
namespace Redis
{


    CHDB_POCO_DECLARE_EXCEPTION(Redis_API, RedisException, Exception)


}
} // namespace CHDBPoco::Redis


#endif // CHDB_Redis_Exception_INCLUDED
