//
// Timezone.cpp
//
// Library: Foundation
// Package: DateTime
// Module:  Timezone
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Timezone.h"
#include <ctime>


#include "Timezone_UNIX.cpp"


namespace DBPoco {


int Timezone::tzd()
{
	return utcOffset() + dst();
}


} // namespace DBPoco
