//
// AbstractExtraction.cpp
//
// Library: Data
// Package: DataCore
// Module:  AbstractExtraction
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Data/AbstractExtraction.h"


namespace CHDBPoco {
namespace Data {


AbstractExtraction::AbstractExtraction(CHDBPoco::UInt32 limit,
	CHDBPoco::UInt32 position,
	bool bulk): 
	_pExtractor(0), 
	_limit(limit),
	_position(position),
	_bulk(bulk),
	_emptyStringIsNull(false),
	_forceEmptyString(false)
{
}


AbstractExtraction::~AbstractExtraction()
{
}


} } // namespace CHDBPoco::Data
