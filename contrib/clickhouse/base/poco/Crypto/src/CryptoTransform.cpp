//
// CryptoTransform.cpp
//
// Library: Crypto
// Package: Cipher
// Module:  CryptoTransform
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Crypto/CryptoTransform.h"


namespace DBPoco {
namespace Crypto {


CryptoTransform::CryptoTransform()
{
}


CryptoTransform::~CryptoTransform()
{
}

  
int CryptoTransform::setPadding(int padding)
{
	return 1;
}


} } // namespace DBPoco::Crypto
