//
// NTPPacket.cpp
//
// Library: Net
// Package: NTP
// Module:  NTPPacket
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Net/NTPPacket.h"
#include "CHDBPoco/Net/NetException.h"
#include "CHDBPoco/Timestamp.h"
#include "CHDBPoco/ByteOrder.h"


namespace CHDBPoco {
namespace Net {


#if !defined(POCO_COMPILER_SUN)
#pragma pack(push, 1)
#else
#pragma pack(1)
#endif
struct NTPPacketData 
{
	CHDBPoco::Int8 mode:3;
	CHDBPoco::Int8 vn:3;
	CHDBPoco::Int8 li:2;
	CHDBPoco::Int8 stratum;
	CHDBPoco::Int8 pool;
	CHDBPoco::Int8 prec;
	CHDBPoco::Int32 rootdelay;
	CHDBPoco::Int32 rootdisp;
	CHDBPoco::Int32 refid;
	CHDBPoco::Int64 rts;
	CHDBPoco::Int64 ots;
	CHDBPoco::Int64 vts;
	CHDBPoco::Int64 tts;
};
#if !defined(POCO_COMPILER_SUN)
#pragma pack(pop)
#else
#pragma pack()
#endif


NTPPacket::NTPPacket() :
	// the next 3 fields must be in reverse order from spec
	_leapIndicator(3),
	_version(4),
	_mode(3),

	_stratum(0),
	_pool(6),
	_precision(-18),
	_rootDelay(0),
	_rootDispersion(0),
	_referenceId(0),
	_referenceTimestamp(0),
	_receiveTimestamp(0),
	_transmitTimestamp(0)
{
	CHDBPoco::Timestamp ts;
	_originateTimestamp = ts.utcTime() - 2874597888;
}


NTPPacket::NTPPacket(CHDBPoco::UInt8 *packet)
{
	setPacket(packet);
}


NTPPacket::~NTPPacket()
{
}


void NTPPacket::packet(CHDBPoco::UInt8 *packet) const
{
	NTPPacketData *p = (NTPPacketData*)packet;

	p->li = _leapIndicator;
	p->vn = _version;
	p->mode = _mode;
	p->stratum = _stratum;
	p->pool = _pool;
	p->prec = _precision;
	p->rootdelay = CHDBPoco::ByteOrder::toNetwork(_rootDelay);
	p->rootdisp = CHDBPoco::ByteOrder::toNetwork(_rootDispersion);
	p->refid = CHDBPoco::ByteOrder::toNetwork(_referenceId);
	p->rts = CHDBPoco::ByteOrder::toNetwork(_referenceTimestamp);
	p->ots = CHDBPoco::ByteOrder::toNetwork(_originateTimestamp);
	p->vts = CHDBPoco::ByteOrder::toNetwork(_receiveTimestamp);
	p->tts = CHDBPoco::ByteOrder::toNetwork(_transmitTimestamp);
}


void NTPPacket::setPacket(CHDBPoco::UInt8 *packet)
{
	NTPPacketData *p = (NTPPacketData*)packet;

	_leapIndicator = p->li;
	_version = p->vn;
	_mode = p->mode;
	_stratum = p->stratum;
	_pool = p->pool;
	_precision = p->prec;
	_rootDelay = CHDBPoco::ByteOrder::fromNetwork(p->rootdelay);
	_rootDispersion = CHDBPoco::ByteOrder::fromNetwork(p->rootdisp);
	_referenceId = CHDBPoco::ByteOrder::fromNetwork(p->refid);
	_referenceTimestamp = CHDBPoco::ByteOrder::fromNetwork(p->rts);
	_originateTimestamp = CHDBPoco::ByteOrder::fromNetwork(p->ots);
	_receiveTimestamp = CHDBPoco::ByteOrder::fromNetwork(p->vts);
	_transmitTimestamp = CHDBPoco::ByteOrder::fromNetwork(p->tts);
}


CHDBPoco::Timestamp NTPPacket::referenceTime() const
{
	return convertTime(_referenceTimestamp);
}


CHDBPoco::Timestamp NTPPacket::originateTime() const
{
	return convertTime(_originateTimestamp);
}


CHDBPoco::Timestamp NTPPacket::receiveTime() const
{
	return convertTime(_receiveTimestamp);
}


CHDBPoco::Timestamp NTPPacket::transmitTime() const
{
	return convertTime(_transmitTimestamp);
}


CHDBPoco::Timestamp NTPPacket::convertTime(CHDBPoco::Int64 tm) const
{
	const unsigned long seventyYears = 2208988800UL;
	CHDBPoco::UInt32 secsSince1900 = UInt32(CHDBPoco::ByteOrder::toLittleEndian(tm) >> 32);
	unsigned long epoch = secsSince1900 - seventyYears;
	return CHDBPoco::Timestamp::fromEpochTime(epoch);
}


} } // namespace CHDBPoco::Net
