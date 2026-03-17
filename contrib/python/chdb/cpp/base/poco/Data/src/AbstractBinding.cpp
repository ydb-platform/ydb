//
// AbstractBinding.cpp
//
// Library: Data
// Package: DataCore
// Module:  AbstractBinding
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Data/AbstractBinding.h"


namespace CHDBPoco {
namespace Data {


AbstractBinding::AbstractBinding(const std::string& name, 
	Direction direction, 
	CHDBPoco::UInt32 bulkSize): 
	_pBinder(0),
	_name(name),
	_direction(direction),
	_bulkSize(bulkSize)
{
}


AbstractBinding::~AbstractBinding()
{
}


void AbstractBinding::setBinder(BinderPtr pBinder)
{
	CHDB_poco_check_ptr (pBinder);
	_pBinder = pBinder;
}


} } // namespace CHDBPoco::Data
