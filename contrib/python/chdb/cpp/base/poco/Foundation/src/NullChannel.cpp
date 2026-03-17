//
// NullChannel.cpp
//
// Library: Foundation
// Package: Logging
// Module:  NullChannel
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/NullChannel.h"


namespace CHDBPoco {


NullChannel::NullChannel()
{
}


NullChannel::~NullChannel()
{
}


void NullChannel::log(const Message&)
{
}


void NullChannel::setProperty(const std::string&, const std::string&)
{
}


} // namespace CHDBPoco
