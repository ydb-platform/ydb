//
// PrivateKeyPassphraseHandler.cpp
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  PrivateKeyPassphraseHandler
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Net/PrivateKeyPassphraseHandler.h"
#include "CHDBPoco/Net/SSLManager.h"
#include "CHDBPoco/Delegate.h"


using CHDBPoco::Delegate;


namespace CHDBPoco {
namespace Net {


PrivateKeyPassphraseHandler::PrivateKeyPassphraseHandler(bool onServerSide): _serverSide(onServerSide)
{
	SSLManager::instance().PrivateKeyPassphraseRequired += Delegate<PrivateKeyPassphraseHandler, std::string>(this, &PrivateKeyPassphraseHandler::onPrivateKeyRequested);
}


PrivateKeyPassphraseHandler::~PrivateKeyPassphraseHandler()
{
	try
	{
		SSLManager::instance().PrivateKeyPassphraseRequired -= Delegate<PrivateKeyPassphraseHandler, std::string>(this, &PrivateKeyPassphraseHandler::onPrivateKeyRequested);
	}
	catch (...)
	{
		CHDB_poco_unexpected();
	}
}


} } // namespace CHDBPoco::Net
