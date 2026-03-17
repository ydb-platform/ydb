//
// ICMPPacketImpl.cpp
//
// Library: Net
// Package: ICMP
// Module:  ICMPPacketImpl
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Net/ICMPPacketImpl.h"
#include "DBPoco/Net/NetException.h"
#include "DBPoco/Timestamp.h"
#include "DBPoco/Timespan.h"
#include "DBPoco/NumberFormatter.h"
#include <sstream>


using DBPoco::InvalidArgumentException;
using DBPoco::Timestamp;
using DBPoco::Timespan;
using DBPoco::NumberFormatter;
using DBPoco::UInt8;
using DBPoco::UInt16;
using DBPoco::Int32;


namespace DBPoco {
namespace Net {


const UInt16 ICMPPacketImpl::MAX_PACKET_SIZE = 4096;
const UInt16 ICMPPacketImpl::MAX_SEQ_VALUE   = 65535;


ICMPPacketImpl::ICMPPacketImpl(int dataSize):
	_seq(0),
	_pPacket(new UInt8[MAX_PACKET_SIZE]),
	_dataSize(dataSize)
{
	if (_dataSize > MAX_PACKET_SIZE)
		throw InvalidArgumentException("Packet size must be <= " + NumberFormatter::format(MAX_PACKET_SIZE));
}


ICMPPacketImpl::~ICMPPacketImpl()
{
	delete [] _pPacket;
}


void ICMPPacketImpl::setDataSize(int dataSize)
{
	_dataSize = dataSize;
	initPacket();
}


int ICMPPacketImpl::getDataSize() const
{
	return _dataSize;
}


const DBPoco::UInt8* ICMPPacketImpl::packet(bool init)
{
	if (init) initPacket();
	return _pPacket;
}


unsigned short ICMPPacketImpl::checksum(UInt16 *addr, Int32 len)
{
	Int32 nleft = len;
	UInt16* w   = addr;
	UInt16 answer;
	Int32 sum = 0;

	while (nleft > 1)  
	{
		sum   += *w++;
		nleft -= sizeof(UInt16);
	}

	if (nleft == 1) 
	{
		UInt16 u = 0;
		*(UInt8*) (&u) = *(UInt8*) w;
		sum += u;
	}

	sum = (sum >> 16) + (sum & 0xffff);
	sum += (sum >> 16);
	answer = ~sum;
	return answer;
}


} } // namespace DBPoco::Net
