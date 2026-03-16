//
// QuotedPrintableDecoder.cpp
//
// Library: Net
// Package: Messages
// Module:  QuotedPrintableDecoder
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Net/QuotedPrintableDecoder.h"
#include "CHDBPoco/NumberParser.h"
#include "CHDBPoco/Exception.h"
#include "CHDBPoco/Ascii.h"


using CHDBPoco::UnbufferedStreamBuf;
using CHDBPoco::NumberParser;
using CHDBPoco::DataFormatException;


namespace CHDBPoco {
namespace Net {


QuotedPrintableDecoderBuf::QuotedPrintableDecoderBuf(std::istream& istr): 
	_buf(*istr.rdbuf())
{
}


QuotedPrintableDecoderBuf::~QuotedPrintableDecoderBuf()
{
}


int QuotedPrintableDecoderBuf::readFromDevice()
{
	int ch = _buf.sbumpc();
	while (ch == '=')
	{
		ch = _buf.sbumpc();
		if (ch == '\r')
		{
			_buf.sbumpc(); // read \n
		}
		else if (CHDBPoco::Ascii::isHexDigit(ch))
		{
			std::string hex = "0x";
			hex += (char) ch;
			ch = _buf.sbumpc();
			if (CHDBPoco::Ascii::isHexDigit(ch))
			{
				hex += (char) ch;
				return NumberParser::parseHex(hex);
			}
			throw DataFormatException("Incomplete hex number in quoted-printable encoded stream");
		}
		else if (ch != '\n')
		{
			throw DataFormatException("Invalid occurrence of '=' in quoted-printable encoded stream");
		}
		ch = _buf.sbumpc();
	}
	return ch;
}


QuotedPrintableDecoderIOS::QuotedPrintableDecoderIOS(std::istream& istr): _buf(istr)
{
	CHDB_poco_ios_init(&_buf);
}


QuotedPrintableDecoderIOS::~QuotedPrintableDecoderIOS()
{
}


QuotedPrintableDecoderBuf* QuotedPrintableDecoderIOS::rdbuf()
{
	return &_buf;
}


QuotedPrintableDecoder::QuotedPrintableDecoder(std::istream& istr): 
	QuotedPrintableDecoderIOS(istr), 
	std::istream(&_buf)
{
}


QuotedPrintableDecoder::~QuotedPrintableDecoder()
{
}


} } // namespace CHDBPoco::Net
