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


#include "DBPoco/Net/NullPartHandler.h"
#include "DBPoco/Net/MessageHeader.h"
#include "DBPoco/NullStream.h"
#include "DBPoco/StreamCopier.h"


using DBPoco::NullOutputStream;
using DBPoco::StreamCopier;


namespace DBPoco {
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


} } // namespace DBPoco::Net
