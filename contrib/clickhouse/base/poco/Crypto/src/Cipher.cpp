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


#include "DBPoco/Crypto/Cipher.h"
#include "DBPoco/Crypto/CryptoStream.h"
#include "DBPoco/Crypto/CryptoTransform.h"
#include "DBPoco/Base64Encoder.h"
#include "DBPoco/Base64Decoder.h"
#include "DBPoco/HexBinaryEncoder.h"
#include "DBPoco/HexBinaryDecoder.h"
#include "DBPoco/StreamCopier.h"
#include "DBPoco/Exception.h"
#include <sstream>
#include <memory>


namespace DBPoco {
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
			DBPoco::Base64Encoder encoder(sink);
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
			DBPoco::HexBinaryEncoder encoder(sink);
			if (encoding == ENC_BINHEX_NO_LF)
			{
				encoder.rdbuf()->setLineLength(0);
			}
			StreamCopier::copyStream(encryptor, encoder);
			encoder.close();
		}
		break;

	default:
		throw DBPoco::InvalidArgumentException("Invalid argument", "encoding");
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
			DBPoco::Base64Decoder decoder(source);
			StreamCopier::copyStream(decoder, decryptor);
			decryptor.close();
		}
		break;

	case ENC_BINHEX:
	case ENC_BINHEX_NO_LF:
		{
			DBPoco::HexBinaryDecoder decoder(source);
			StreamCopier::copyStream(decoder, decryptor);
			decryptor.close();
		}
		break;

	default:
		throw DBPoco::InvalidArgumentException("Invalid argument", "encoding");
	}
}


} } // namespace DBPoco::Crypto
