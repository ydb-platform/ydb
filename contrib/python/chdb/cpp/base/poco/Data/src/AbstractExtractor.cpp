//
// AbstractExtractor.cpp
//
// Library: Data
// Package: DataCore
// Module:  AbstractExtractor
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Data/AbstractExtractor.h"
#include "CHDBPoco/Exception.h"


namespace CHDBPoco {
namespace Data {


AbstractExtractor::AbstractExtractor()
{
}


AbstractExtractor::~AbstractExtractor()
{
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<CHDBPoco::Int8>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<CHDBPoco::Int8>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<CHDBPoco::Int8>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<CHDBPoco::UInt8>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<CHDBPoco::UInt8>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<CHDBPoco::UInt8>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<CHDBPoco::Int16>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<CHDBPoco::Int16>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<CHDBPoco::Int16>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<CHDBPoco::UInt16>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<CHDBPoco::UInt16>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<CHDBPoco::UInt16>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<CHDBPoco::Int32>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<CHDBPoco::Int32>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<CHDBPoco::Int32>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<CHDBPoco::UInt32>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<CHDBPoco::UInt32>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<CHDBPoco::UInt32>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<CHDBPoco::Int64>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<CHDBPoco::Int64>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<CHDBPoco::Int64>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<CHDBPoco::UInt64>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<CHDBPoco::UInt64>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<CHDBPoco::UInt64>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


#ifndef CHDB_POCO_LONG_IS_64_BIT
bool AbstractExtractor::extract(std::size_t pos, std::vector<long>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<long>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<long>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}
#endif


bool AbstractExtractor::extract(std::size_t pos, std::vector<bool>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<bool>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<bool>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<float>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<float>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<float>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<double>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<double>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<double>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<char>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<char>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<char>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<std::string>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<std::string>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<std::string>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, UTF16String& val)
{
	throw NotImplementedException("UTF16String extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<UTF16String>& val)
{
	throw NotImplementedException("std::vector<UTF16String> extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<UTF16String>& val)
{
	throw NotImplementedException("std::deque<UTF16String> extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<UTF16String>& val)
{
	throw NotImplementedException("std::list<UTF16String> extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<BLOB>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<BLOB>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<BLOB>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<CLOB>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<CLOB>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<CLOB>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<DateTime>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<DateTime>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<DateTime>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<Date>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<Date>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<Date>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<Time>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<Time>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<Time>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<Any>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<Any>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<Any>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::vector<CHDBPoco::Dynamic::Var>& val)
{
	throw NotImplementedException("std::vector extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::deque<CHDBPoco::Dynamic::Var>& val)
{
	throw NotImplementedException("std::deque extractor must be implemented.");
}


bool AbstractExtractor::extract(std::size_t pos, std::list<CHDBPoco::Dynamic::Var>& val)
{
	throw NotImplementedException("std::list extractor must be implemented.");
}


} } // namespace CHDBPoco::Data
