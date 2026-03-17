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


#include "DBPoco/Net/ICMPPacket.h"
#include "DBPoco/Net/ICMPv4PacketImpl.h"
#include "DBPoco/Net/NetException.h"
#include "DBPoco/Timestamp.h"
#include "DBPoco/Timespan.h"
#include "DBPoco/NumberFormatter.h"
#include <sstream>


using DBPoco::InvalidArgumentException;
using DBPoco::NotImplementedException;
using DBPoco::Timestamp;
using DBPoco::Timespan;
using DBPoco::NumberFormatter;
using DBPoco::UInt8;
using DBPoco::UInt16;
using DBPoco::Int32;


namespace DBPoco {
namespace Net {


ICMPPacket::ICMPPacket(IPAddress::Family family, int dataSize):_pImpl(0)
{
	if (family == IPAddress::IPv4)
		_pImpl = new ICMPv4PacketImpl(dataSize);
#if defined(DB_POCO_HAVE_IPv6)
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


const DBPoco::UInt8* ICMPPacket::packet()
{
	return _pImpl->packet();
}


struct timeval ICMPPacket::time(DBPoco::UInt8* buffer, int length) const
{
	return _pImpl->time(buffer, length);
}


bool ICMPPacket::validReplyID(DBPoco::UInt8* buffer, int length) const
{
	return _pImpl->validReplyID(buffer, length);
}


std::string ICMPPacket::errorDescription(DBPoco::UInt8* buffer, int length)
{
	return _pImpl->errorDescription(buffer, length);
}


std::string ICMPPacket::typeDescription(int typeId)
{
	return _pImpl->typeDescription(typeId);
}


} } // namespace DBPoco::Net
