//
// KeyPair.cpp
//
//
// Library: Crypto
// Package: CryptoCore
// Module:  KeyPair
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Crypto/KeyPair.h"
#include <openssl/rsa.h>


namespace CHDBPoco {
namespace Crypto {


KeyPair::KeyPair(KeyPairImpl::Ptr pKeyPairImpl): _pImpl(pKeyPairImpl)
{
}


KeyPair::~KeyPair()
{
}


} } // namespace CHDBPoco::Crypto
