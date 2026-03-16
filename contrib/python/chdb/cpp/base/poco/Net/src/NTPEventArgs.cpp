//
// NTPEventArgs.cpp
//
// Library: Net
// Package: NTP
// Module:  NTPEventArgs
//
// Implementation of NTPEventArgs
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Net/NTPEventArgs.h"
#include "CHDBPoco/Net/SocketAddress.h"
#include "CHDBPoco/Net/DNS.h"
#include "CHDBPoco/Exception.h"
#include "CHDBPoco/Net/NetException.h"


using CHDBPoco::IOException;
using CHDBPoco::InvalidArgumentException;


namespace CHDBPoco {
namespace Net {


NTPEventArgs::NTPEventArgs(const SocketAddress& address):
	_address(address), _packet()
{
}


NTPEventArgs::~NTPEventArgs()
{
}


std::string NTPEventArgs::hostName() const
{
	try
	{
		return DNS::resolve(_address.host().toString()).name();
	}
	catch (HostNotFoundException&) 
	{
	}
	catch (NoAddressFoundException&) 
	{
	}
	catch (DNSException&)
	{
	}
	catch (IOException&)
	{
	}
	return _address.host().toString();
}


std::string NTPEventArgs::hostAddress() const
{
	return _address.host().toString();
}


} } // namespace CHDBPoco::Net
