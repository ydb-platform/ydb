//
// AbstractPreparator.cpp
//
// Library: Data
// Package: DataCore
// Module:  AbstractPreparator
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Data/AbstractPreparator.h"


namespace CHDBPoco {
namespace Data {


AbstractPreparator::AbstractPreparator(CHDBPoco::UInt32 length): 
	_length(length),
	_bulk(false)
{
}


AbstractPreparator::~AbstractPreparator()
{
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<CHDBPoco::Int8>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<CHDBPoco::Int8>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<CHDBPoco::Int8>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<CHDBPoco::UInt8>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<CHDBPoco::UInt8>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<CHDBPoco::UInt8>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<CHDBPoco::Int16>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<CHDBPoco::Int16>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<CHDBPoco::Int16>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<CHDBPoco::UInt16>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<CHDBPoco::UInt16>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<CHDBPoco::UInt16>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<CHDBPoco::Int32>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<CHDBPoco::Int32>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<CHDBPoco::Int32>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<CHDBPoco::UInt32>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<CHDBPoco::UInt32>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<CHDBPoco::UInt32>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<CHDBPoco::Int64>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<CHDBPoco::Int64>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<CHDBPoco::Int64>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<CHDBPoco::UInt64>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<CHDBPoco::UInt64>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<CHDBPoco::UInt64>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


#ifndef CHDB_POCO_LONG_IS_64_BIT
void AbstractPreparator::prepare(std::size_t pos, const std::vector<long>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<long>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<long>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}
#endif


void AbstractPreparator::prepare(std::size_t pos, const std::vector<bool>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<bool>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<bool>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<float>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<float>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<float>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<double>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<double>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<double>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<char>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<char>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<char>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<std::string>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<std::string>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<std::string>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const UTF16String& val)
{
	throw NotImplementedException("UTF16String preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<UTF16String>& val)
{
	throw NotImplementedException("std::vector<UTF16String> preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<UTF16String>& val)
{
	throw NotImplementedException("std::deque<UTF16String> preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<UTF16String>& val)
{
	throw NotImplementedException("std::list<UTF16String> preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<BLOB>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<BLOB>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<BLOB>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<CLOB>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<CLOB>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<CLOB>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<DateTime>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<DateTime>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<DateTime>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<Date>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<Date>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<Date>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<Time>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<Time>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<Time>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<Any>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<Any>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<Any>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::vector<CHDBPoco::Dynamic::Var>& val)
{
	throw NotImplementedException("std::vector preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::deque<CHDBPoco::Dynamic::Var>& val)
{
	throw NotImplementedException("std::deque preparator must be implemented.");
}


void AbstractPreparator::prepare(std::size_t pos, const std::list<CHDBPoco::Dynamic::Var>& val)
{
	throw NotImplementedException("std::list preparator must be implemented.");
}


} } // namespace CHDBPoco::Data
