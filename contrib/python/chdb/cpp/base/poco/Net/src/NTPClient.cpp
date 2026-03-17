//
// NTPClient.cpp
//
// Library: Net
// Package: NTP
// Module:  NTPClient
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Net/SocketAddress.h"
#include "CHDBPoco/Net/NTPClient.h"
#include "CHDBPoco/Net/NTPPacket.h"
#include "CHDBPoco/Net/DatagramSocket.h"
#include "CHDBPoco/Net/NetException.h"


using CHDBPoco::TimeoutException;


namespace CHDBPoco {
namespace Net {


NTPClient::NTPClient(IPAddress::Family family, int timeout): 
	_family(family), _timeout(timeout)
{
}


NTPClient::~NTPClient()
{
}


int NTPClient::request(const std::string& address) const
{
	SocketAddress addr(address, 123);
	return request(addr);
}


int NTPClient::request(SocketAddress& address) const
{
	CHDBPoco::Net::SocketAddress sa;
	DatagramSocket ntpSocket(_family);
	ntpSocket.setReceiveTimeout(_timeout);
	ntpSocket.bind(sa);

	SocketAddress returnAddress;

	NTPEventArgs eventArgs(address);

	NTPPacket packet;
	CHDBPoco::UInt8 p[1024];
	packet.packet(&p[0]);

	ntpSocket.sendTo(p, 48, address);

	int received = 0;
	try
	{
		CHDBPoco::Net::SocketAddress sender;
		int n = ntpSocket.receiveFrom(p, sizeof(p)-1, sender);

		if (n < 48) // NTP packet must have at least 48 bytes
			throw CHDBPoco::Net::NTPException("Invalid response received");

		packet.setPacket(p);
		eventArgs.setPacket(packet);
		++received;
		response.notify(this, eventArgs);
	}
	catch (CHDBPoco::TimeoutException &)
	{
		// ignore
	}

	return received;
}


} } // namespace CHDBPoco::Net
