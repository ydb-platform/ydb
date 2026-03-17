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


#include "DBPoco/Net/NTPPacket.h"
#include "DBPoco/Net/NetException.h"
#include "DBPoco/Timestamp.h"
#include "DBPoco/ByteOrder.h"


namespace DBPoco {
namespace Net {


#if !defined(POCO_COMPILER_SUN)
#pragma pack(push, 1)
#else
#pragma pack(1)
#endif
struct NTPPacketData 
{
	DBPoco::Int8 mode:3;
	DBPoco::Int8 vn:3;
	DBPoco::Int8 li:2;
	DBPoco::Int8 stratum;
	DBPoco::Int8 pool;
	DBPoco::Int8 prec;
	DBPoco::Int32 rootdelay;
	DBPoco::Int32 rootdisp;
	DBPoco::Int32 refid;
	DBPoco::Int64 rts;
	DBPoco::Int64 ots;
	DBPoco::Int64 vts;
	DBPoco::Int64 tts;
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
	DBPoco::Timestamp ts;
	_originateTimestamp = ts.utcTime() - 2874597888;
}


NTPPacket::NTPPacket(DBPoco::UInt8 *packet)
{
	setPacket(packet);
}


NTPPacket::~NTPPacket()
{
}


void NTPPacket::packet(DBPoco::UInt8 *packet) const
{
	NTPPacketData *p = (NTPPacketData*)packet;

	p->li = _leapIndicator;
	p->vn = _version;
	p->mode = _mode;
	p->stratum = _stratum;
	p->pool = _pool;
	p->prec = _precision;
	p->rootdelay = DBPoco::ByteOrder::toNetwork(_rootDelay);
	p->rootdisp = DBPoco::ByteOrder::toNetwork(_rootDispersion);
	p->refid = DBPoco::ByteOrder::toNetwork(_referenceId);
	p->rts = DBPoco::ByteOrder::toNetwork(_referenceTimestamp);
	p->ots = DBPoco::ByteOrder::toNetwork(_originateTimestamp);
	p->vts = DBPoco::ByteOrder::toNetwork(_receiveTimestamp);
	p->tts = DBPoco::ByteOrder::toNetwork(_transmitTimestamp);
}


void NTPPacket::setPacket(DBPoco::UInt8 *packet)
{
	NTPPacketData *p = (NTPPacketData*)packet;

	_leapIndicator = p->li;
	_version = p->vn;
	_mode = p->mode;
	_stratum = p->stratum;
	_pool = p->pool;
	_precision = p->prec;
	_rootDelay = DBPoco::ByteOrder::fromNetwork(p->rootdelay);
	_rootDispersion = DBPoco::ByteOrder::fromNetwork(p->rootdisp);
	_referenceId = DBPoco::ByteOrder::fromNetwork(p->refid);
	_referenceTimestamp = DBPoco::ByteOrder::fromNetwork(p->rts);
	_originateTimestamp = DBPoco::ByteOrder::fromNetwork(p->ots);
	_receiveTimestamp = DBPoco::ByteOrder::fromNetwork(p->vts);
	_transmitTimestamp = DBPoco::ByteOrder::fromNetwork(p->tts);
}


DBPoco::Timestamp NTPPacket::referenceTime() const
{
	return convertTime(_referenceTimestamp);
}


DBPoco::Timestamp NTPPacket::originateTime() const
{
	return convertTime(_originateTimestamp);
}


DBPoco::Timestamp NTPPacket::receiveTime() const
{
	return convertTime(_receiveTimestamp);
}


DBPoco::Timestamp NTPPacket::transmitTime() const
{
	return convertTime(_transmitTimestamp);
}


DBPoco::Timestamp NTPPacket::convertTime(DBPoco::Int64 tm) const
{
	const unsigned long seventyYears = 2208988800UL;
	DBPoco::UInt32 secsSince1900 = UInt32(DBPoco::ByteOrder::toLittleEndian(tm) >> 32);
	unsigned long epoch = secsSince1900 - seventyYears;
	return DBPoco::Timestamp::fromEpochTime(epoch);
}


} } // namespace DBPoco::Net
