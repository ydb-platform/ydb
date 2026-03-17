//
// NullPartHandler.cpp
//
// Library: Net
// Package: Messages
// Module:  NullPartHandler
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Net/NullPartHandler.h"
#include "CHDBPoco/Net/MessageHeader.h"
#include "CHDBPoco/NullStream.h"
#include "CHDBPoco/StreamCopier.h"


using CHDBPoco::NullOutputStream;
using CHDBPoco::StreamCopier;


namespace CHDBPoco {
namespace Net {


NullPartHandler::NullPartHandler()
{
}


NullPartHandler::~NullPartHandler()
{
}


void NullPartHandler::handlePart(const MessageHeader& header, std::istream& stream)
{
	NullOutputStream ostr;
	StreamCopier::copyStream(stream, ostr);
}


} } // namespace CHDBPoco::Net
