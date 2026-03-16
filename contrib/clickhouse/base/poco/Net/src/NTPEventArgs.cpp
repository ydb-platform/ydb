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


#include "DBPoco/Net/NTPEventArgs.h"
#include "DBPoco/Net/SocketAddress.h"
#include "DBPoco/Net/DNS.h"
#include "DBPoco/Exception.h"
#include "DBPoco/Net/NetException.h"


using DBPoco::IOException;
using DBPoco::InvalidArgumentException;


namespace DBPoco {
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


} } // namespace DBPoco::Net
