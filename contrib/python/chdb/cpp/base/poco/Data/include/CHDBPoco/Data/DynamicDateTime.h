//
// DynamicDateTime.h
//
// Library: Data
// Package: DataCore
// Module:  DynamicDateTime
//
// Definition of the Date and Time cast operators for CHDBPoco::Dynamic::Var.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Data_DynamicDateTime_INCLUDED
#define CHDB_Data_DynamicDateTime_INCLUDED


#include "CHDBPoco/Data/Data.h"
#include "CHDBPoco/Data/Date.h"
#include "CHDBPoco/Data/Time.h"
#include "CHDBPoco/Dynamic/Var.h"


namespace CHDBPoco
{
namespace Data
{

    class Date;
    class Time;

}
} // namespace CHDBPoco::Data


namespace CHDBPoco
{
namespace Dynamic
{


    template <>
    Data_API Var::operator CHDBPoco::Data::Date() const;
    template <>
    Data_API Var::operator CHDBPoco::Data::Time() const;


}
} // namespace CHDBPoco::Dynamic


#endif // CHDB_Data_DynamicDateTime_INCLUDED
