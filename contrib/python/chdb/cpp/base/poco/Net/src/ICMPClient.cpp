//
// ICMPClient.cpp
//
// Library: Net
// Package: ICMP
// Module:  ICMPClient
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Net/SocketAddress.h"
#include "CHDBPoco/Net/ICMPClient.h"
#include "CHDBPoco/Net/ICMPSocket.h"
#include "CHDBPoco/Net/NetException.h"
#include "CHDBPoco/Channel.h"
#include "CHDBPoco/Message.h"
#include "CHDBPoco/Exception.h"
#include <sstream>


using CHDBPoco::Channel;
using CHDBPoco::Message;
using CHDBPoco::InvalidArgumentException;
using CHDBPoco::NotImplementedException;
using CHDBPoco::TimeoutException;
using CHDBPoco::Exception;


namespace CHDBPoco {
namespace Net {


ICMPClient::ICMPClient(SocketAddress::Family family, int dataSize, int ttl, int timeout):
	_family(family),
	_dataSize(dataSize),
	_ttl(ttl),
	_timeout(timeout)
{
}


ICMPClient::~ICMPClient()
{
}


int ICMPClient::ping(const std::string& address, int repeat) const
{
	if (repeat <= 0) return 0;

	SocketAddress addr(address, 0);
	return ping(addr, repeat);
}


int ICMPClient::ping(SocketAddress& address, int repeat) const
{
	if (repeat <= 0) return 0;

	ICMPSocket icmpSocket(_family, _dataSize, _ttl, _timeout);
	SocketAddress returnAddress;

	ICMPEventArgs eventArgs(address, repeat, icmpSocket.dataSize(), icmpSocket.ttl());
	pingBegin.notify(this, eventArgs);

	for (int i = 0; i < repeat; ++i)
	{
		icmpSocket.sendTo(address);
		++eventArgs;

		try
		{
			int t = icmpSocket.receiveFrom(returnAddress);
			eventArgs.setReplyTime(i, t);
			pingReply.notify(this, eventArgs);
		}
		catch (TimeoutException&)
		{
			std::ostringstream os;
			os << address.host().toString() << ": Request timed out.";
			eventArgs.setError(i, os.str());
			pingError.notify(this, eventArgs);
			continue;
		}
		catch (ICMPException& ex)
		{
			std::ostringstream os;
			os << address.host().toString() << ": " << ex.what();
			eventArgs.setError(i, os.str());
			pingError.notify(this, eventArgs);
			continue;
		}
		catch (Exception& ex)
		{
			std::ostringstream os;
			os << ex.displayText();
			eventArgs.setError(i, os.str());
			pingError.notify(this, eventArgs);
			continue;
		}
	}
	pingEnd.notify(this, eventArgs);
	return eventArgs.received();
}


int ICMPClient::pingIPv4(const std::string& address, int repeat,
	int dataSize, int ttl, int timeout)
{
	if (repeat <= 0) return 0;

	SocketAddress a(address, 0);
	return ping(a, IPAddress::IPv4, repeat, dataSize, ttl, timeout);
}


int ICMPClient::ping(SocketAddress& address,
	IPAddress::Family family, int repeat,
	int dataSize, int ttl, int timeout)
{
	if (repeat <= 0) return 0;

	ICMPSocket icmpSocket(family, dataSize, ttl, timeout);
	SocketAddress returnAddress;
	int received = 0;

	for (int i = 0; i < repeat; ++i)
	{
		icmpSocket.sendTo(address);
		try
		{
			icmpSocket.receiveFrom(returnAddress);
			++received;
		}
		catch (TimeoutException&)
		{
		}
		catch (ICMPException&)
		{
		}
	}
	return received;
}


} } // namespace CHDBPoco::Net
