//
// KeyPairImpl.cpp
//
//
// Library: Crypto
// Package: CryptoCore
// Module:  KeyPairImpl
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Crypto/KeyPairImpl.h"


namespace CHDBPoco {
namespace Crypto {


KeyPairImpl::KeyPairImpl(const std::string& name, Type type):
	_name(name),
	_type(type)
{
}


KeyPairImpl::~KeyPairImpl()
{
}


} } // namespace CHDBPoco::Crypto
