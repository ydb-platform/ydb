//
// AbstractPreparation.cpp
//
// Library: Data
// Package: DataCore
// Module:  AbstractPreparation
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Data/AbstractPreparation.h"


namespace CHDBPoco {
namespace Data {


AbstractPreparation::AbstractPreparation(PreparatorPtr pPreparator):
	_pPreparator(pPreparator)
{
	CHDB_poco_assert_dbg (_pPreparator);
}


AbstractPreparation::~AbstractPreparation()
{
}


} } // namespace CHDBPoco::Data
