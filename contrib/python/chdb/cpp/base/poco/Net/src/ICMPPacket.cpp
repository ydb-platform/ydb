//
// ICMPPacket.cpp
//
// Library: Net
// Package: ICMP
// Module:  ICMPPacket
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Net/ICMPPacket.h"
#include "CHDBPoco/Net/ICMPv4PacketImpl.h"
#include "CHDBPoco/Net/NetException.h"
#include "CHDBPoco/Timestamp.h"
#include "CHDBPoco/Timespan.h"
#include "CHDBPoco/NumberFormatter.h"
#include <sstream>


using CHDBPoco::InvalidArgumentException;
using CHDBPoco::NotImplementedException;
using CHDBPoco::Timestamp;
using CHDBPoco::Timespan;
using CHDBPoco::NumberFormatter;
using CHDBPoco::UInt8;
using CHDBPoco::UInt16;
using CHDBPoco::Int32;


namespace CHDBPoco {
namespace Net {


ICMPPacket::ICMPPacket(IPAddress::Family family, int dataSize):_pImpl(0)
{
	if (family == IPAddress::IPv4)
		_pImpl = new ICMPv4PacketImpl(dataSize);
#if defined(CHDB_POCO_HAVE_IPv6)
	else if (family == IPAddress::IPv6)
		throw NotImplementedException("ICMPv6 packets not implemented.");
#endif
	else throw InvalidArgumentException("Invalid or unsupported address family passed to ICMPPacket");
}


ICMPPacket::~ICMPPacket()
{
	delete _pImpl;
}


void ICMPPacket::setDataSize(int dataSize)
{
	_pImpl->setDataSize(dataSize);
}


int ICMPPacket::getDataSize() const
{
	return _pImpl->getDataSize();
}


int ICMPPacket::packetSize() const
{
	return _pImpl->packetSize();
}


int ICMPPacket::maxPacketSize() const
{
	return _pImpl->maxPacketSize();
}


const CHDBPoco::UInt8* ICMPPacket::packet()
{
	return _pImpl->packet();
}


struct timeval ICMPPacket::time(CHDBPoco::UInt8* buffer, int length) const
{
	return _pImpl->time(buffer, length);
}


bool ICMPPacket::validReplyID(CHDBPoco::UInt8* buffer, int length) const
{
	return _pImpl->validReplyID(buffer, length);
}


std::string ICMPPacket::errorDescription(CHDBPoco::UInt8* buffer, int length)
{
	return _pImpl->errorDescription(buffer, length);
}


std::string ICMPPacket::typeDescription(int typeId)
{
	return _pImpl->typeDescription(typeId);
}


} } // namespace CHDBPoco::Net
