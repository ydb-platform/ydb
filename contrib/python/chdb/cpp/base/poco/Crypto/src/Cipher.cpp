//
// Cipher.cpp
//
// Library: Crypto
// Package: Cipher
// Module:  Cipher
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Crypto/Cipher.h"
#include "CHDBPoco/Crypto/CryptoStream.h"
#include "CHDBPoco/Crypto/CryptoTransform.h"
#include "CHDBPoco/Base64Encoder.h"
#include "CHDBPoco/Base64Decoder.h"
#include "CHDBPoco/HexBinaryEncoder.h"
#include "CHDBPoco/HexBinaryDecoder.h"
#include "CHDBPoco/StreamCopier.h"
#include "CHDBPoco/Exception.h"
#include <sstream>
#include <memory>


namespace CHDBPoco {
namespace Crypto {


Cipher::Cipher()
{
}


Cipher::~Cipher()
{
}


std::string Cipher::encryptString(const std::string& str, Encoding encoding)
{
	std::istringstream source(str);
	std::ostringstream sink;

	encrypt(source, sink, encoding);

	return sink.str();
}


std::string Cipher::decryptString(const std::string& str, Encoding encoding)
{
	std::istringstream source(str);
	std::ostringstream sink;

	decrypt(source, sink, encoding);
	return sink.str();
}


void Cipher::encrypt(std::istream& source, std::ostream& sink, Encoding encoding)
{
	CryptoInputStream encryptor(source, createEncryptor());

	switch (encoding)
	{
	case ENC_NONE:
		StreamCopier::copyStream(encryptor, sink);
		break;

	case ENC_BASE64:
	case ENC_BASE64_NO_LF:
		{
			CHDBPoco::Base64Encoder encoder(sink);
			if (encoding == ENC_BASE64_NO_LF)
			{
				encoder.rdbuf()->setLineLength(0);
			}
			StreamCopier::copyStream(encryptor, encoder);
			encoder.close();
		}
		break;

	case ENC_BINHEX:
	case ENC_BINHEX_NO_LF:
		{
			CHDBPoco::HexBinaryEncoder encoder(sink);
			if (encoding == ENC_BINHEX_NO_LF)
			{
				encoder.rdbuf()->setLineLength(0);
			}
			StreamCopier::copyStream(encryptor, encoder);
			encoder.close();
		}
		break;

	default:
		throw CHDBPoco::InvalidArgumentException("Invalid argument", "encoding");
	}
}


void Cipher::decrypt(std::istream& source, std::ostream& sink, Encoding encoding)
{
	CryptoOutputStream decryptor(sink, createDecryptor());

	switch (encoding)
	{
	case ENC_NONE:
		StreamCopier::copyStream(source, decryptor);
		decryptor.close();
		break;

	case ENC_BASE64:
	case ENC_BASE64_NO_LF:
		{
			CHDBPoco::Base64Decoder decoder(source);
			StreamCopier::copyStream(decoder, decryptor);
			decryptor.close();
		}
		break;

	case ENC_BINHEX:
	case ENC_BINHEX_NO_LF:
		{
			CHDBPoco::HexBinaryDecoder decoder(source);
			StreamCopier::copyStream(decoder, decryptor);
			decryptor.close();
		}
		break;

	default:
		throw CHDBPoco::InvalidArgumentException("Invalid argument", "encoding");
	}
}


} } // namespace CHDBPoco::Crypto
