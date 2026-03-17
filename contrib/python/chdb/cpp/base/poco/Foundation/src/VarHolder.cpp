//
// VarHolder.cpp
//
// Library: Foundation
// Package: Core
// Module:  VarHolder
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Dynamic/VarHolder.h"
#include "CHDBPoco/Dynamic/Var.h"
#include "CHDBPoco/JSONString.h"


namespace CHDBPoco {
namespace Dynamic {


VarHolder::VarHolder()
{
}


VarHolder::~VarHolder()
{
}


namespace Impl {


void escape(std::string& target, const std::string& source)
{
	target = toJSON(source);
}


bool isJSONString(const Var& any)
{
	return any.type() == typeid(std::string) ||
		any.type() == typeid(char) ||
		any.type() == typeid(char*) ||
		any.type() == typeid(CHDBPoco::DateTime) ||
		any.type() == typeid(CHDBPoco::LocalDateTime) ||
		any.type() == typeid(CHDBPoco::Timestamp);
}


void appendJSONString(std::string& val, const Var& any)
{
	std::string json;
	escape(json, any.convert<std::string>());
	val.append(json);
}


void appendJSONKey(std::string& val, const Var& any)
{
	return appendJSONString(val, any);
}


void appendJSONValue(std::string& val, const Var& any)
{
	if (any.isEmpty())
	{
		val.append("null");
	}
	else
	{
		bool isStr = isJSONString(any);
		if (isStr)
		{
			appendJSONString(val, any.convert<std::string>());
		}
		else
		{
			val.append(any.convert<std::string>());
		}
	}
}


} // namespace Impl


} } // namespace CHDBPoco::Dynamic
