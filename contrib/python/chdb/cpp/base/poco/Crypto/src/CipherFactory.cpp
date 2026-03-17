//
// CipherFactory.cpp
//
// Library: Crypto
// Package: Cipher
// Module:  CipherFactory
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Crypto/CipherFactory.h"
#include "CHDBPoco/Crypto/Cipher.h"
#include "CHDBPoco/Crypto/CipherKey.h"
#include "CHDBPoco/Crypto/RSAKey.h"
#include "CHDBPoco/Crypto/CipherImpl.h"
#include "CHDBPoco/Crypto/RSACipherImpl.h"
#include "CHDBPoco/Exception.h"
#include "CHDBPoco/SingletonHolder.h"
#include <openssl/evp.h>
#include <openssl/err.h>


namespace CHDBPoco {
namespace Crypto {


CipherFactory::CipherFactory()
{
}


CipherFactory::~CipherFactory()
{
}


namespace
{
	static CHDBPoco::SingletonHolder<CipherFactory> holder;
}


CipherFactory& CipherFactory::defaultFactory()
{
	return *holder.get();
}


Cipher* CipherFactory::createCipher(const CipherKey& key)
{
	return new CipherImpl(key);
}


Cipher* CipherFactory::createCipher(const RSAKey& key, RSAPaddingMode paddingMode)
{
	return new RSACipherImpl(key, paddingMode);
}


} } // namespace CHDBPoco::Crypto
