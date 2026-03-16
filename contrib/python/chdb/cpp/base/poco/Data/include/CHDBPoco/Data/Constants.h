//
// Data.h
//
// Library: Data
// Package: DataCore
// Module:  Constants
//
// Constant definitions for the Poco Data library.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Data_Constants_INCLUDED
#define CHDB_Data_Constants_INCLUDED


#undef max
#include <cstddef>
#include <limits>


namespace CHDBPoco
{
namespace Data
{


    static const std::size_t POCO_DATA_INVALID_ROW = std::numeric_limits<std::size_t>::max();


}
} // namespace CHDBPoco::Data


#endif // CHDB_Data_Constants_INCLUDED
