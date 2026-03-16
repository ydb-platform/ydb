//
// ICMPSocketImpl.cpp
//
// Library: Net
// Package: ICMP
// Module:  ICMPSocketImpl
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Net/ICMPSocketImpl.h"
#include "DBPoco/Net/NetException.h"
#include "DBPoco/Timespan.h"
#include "DBPoco/Timestamp.h"
#include "DBPoco/Exception.h"
#include "DBPoco/Buffer.h"


using DBPoco::TimeoutException;
using DBPoco::Timespan;
using DBPoco::Exception;


namespace DBPoco {
namespace Net {


ICMPSocketImpl::ICMPSocketImpl(IPAddress::Family family, int dataSize, int ttl, int timeout):
	RawSocketImpl(family, IPPROTO_ICMP),
	_icmpPacket(family, dataSize),
	_ttl(ttl),
	_timeout(timeout)
{
	setOption(IPPROTO_IP, IP_TTL, ttl);
	setReceiveTimeout(Timespan(timeout));
}


ICMPSocketImpl::~ICMPSocketImpl()
{
}


int ICMPSocketImpl::sendTo(const void*, int, const SocketAddress& address, int flags)
{
	int n = SocketImpl::sendTo(_icmpPacket.packet(), _icmpPacket.packetSize(), address, flags);
	return n;
}


int ICMPSocketImpl::receiveFrom(void*, int, SocketAddress& address, int flags)
{
	int maxPacketSize = _icmpPacket.maxPacketSize();
	DBPoco::Buffer<unsigned char> buffer(maxPacketSize);

	try
	{
		DBPoco::Timestamp ts;
		do
		{
			if (ts.isElapsed(_timeout))
			{
				// This guards against a possible DoS attack, where sending
				// fake ping responses will cause an endless loop.
				throw TimeoutException();
			}
			SocketImpl::receiveFrom(buffer.begin(), maxPacketSize, address, flags);
		}
		while (!_icmpPacket.validReplyID(buffer.begin(), maxPacketSize));
	}
	catch (TimeoutException&)
	{
		throw;
	}
	catch (Exception&)
	{
		std::string err = _icmpPacket.errorDescription(buffer.begin(), maxPacketSize);
		if (!err.empty())
			throw ICMPException(err);
		else
			throw;
	}

	struct timeval then = _icmpPacket.time(buffer.begin(), maxPacketSize);
	struct timeval now  = _icmpPacket.time();

	int elapsed	= (((now.tv_sec * 1000000) + now.tv_usec) - ((then.tv_sec * 1000000) + then.tv_usec))/1000;

	return elapsed;
}


} } // namespace DBPoco::Net
