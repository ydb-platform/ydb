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


#include "DBPoco/Net/PrivateKeyPassphraseHandler.h"
#include "DBPoco/Net/SSLManager.h"
#include "DBPoco/Delegate.h"


using DBPoco::Delegate;


namespace DBPoco {
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
		DB_poco_unexpected();
	}
}


} } // namespace DBPoco::Net
