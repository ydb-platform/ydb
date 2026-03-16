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


#include "CHDBPoco/Timezone.h"
#include <ctime>


#include "Timezone_UNIX.cpp"


namespace CHDBPoco {


int Timezone::tzd()
{
	return utcOffset() + dst();
}


} // namespace CHDBPoco
