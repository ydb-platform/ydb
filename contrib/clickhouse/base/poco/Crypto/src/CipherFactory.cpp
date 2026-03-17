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


#include "DBPoco/Crypto/CipherFactory.h"
#include "DBPoco/Crypto/Cipher.h"
#include "DBPoco/Crypto/CipherKey.h"
#include "DBPoco/Crypto/RSAKey.h"
#include "DBPoco/Crypto/CipherImpl.h"
#include "DBPoco/Crypto/RSACipherImpl.h"
#include "DBPoco/Exception.h"
#include "DBPoco/SingletonHolder.h"
#include <openssl/evp.h>
#include <openssl/err.h>


namespace DBPoco {
namespace Crypto {


CipherFactory::CipherFactory()
{
}


CipherFactory::~CipherFactory()
{
}


namespace
{
	static DBPoco::SingletonHolder<CipherFactory> holder;
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


} } // namespace DBPoco::Crypto
