//
// Bulk.cpp
//
// Library: Data
// Package: DataCore
// Module:  Bulk
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Data/Bulk.h"


namespace CHDBPoco {
namespace Data {


Bulk::Bulk(const Limit& limit): _limit(limit.value(), false, false)
{
}


Bulk::Bulk(CHDBPoco::UInt32 value): _limit(value, false, false)
{
}


Bulk::~Bulk()
{
}


} } // namespace CHDBPoco::Data
