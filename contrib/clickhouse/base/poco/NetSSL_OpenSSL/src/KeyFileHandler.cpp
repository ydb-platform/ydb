//
// KeyFileHandler.cpp
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  KeyFileHandler
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Net/KeyFileHandler.h"
#include "DBPoco/Net/SSLManager.h"
#include "DBPoco/File.h"
#include "DBPoco/Util/AbstractConfiguration.h"
#include "DBPoco/Util/Application.h"
#include "DBPoco/Util/OptionException.h"


namespace DBPoco {
namespace Net {


const std::string KeyFileHandler::CFG_PRIV_KEY_FILE("privateKeyPassphraseHandler.options.password");


KeyFileHandler::KeyFileHandler(bool server):PrivateKeyPassphraseHandler(server)
{
}


KeyFileHandler::~KeyFileHandler()
{
}


void KeyFileHandler::onPrivateKeyRequested(const void* pSender, std::string& privateKey)
{
	try
	{
		DBPoco::Util::AbstractConfiguration& config = DBPoco::Util::Application::instance().config();
		std::string prefix = serverSide() ? SSLManager::CFG_SERVER_PREFIX : SSLManager::CFG_CLIENT_PREFIX;
		if (!config.hasProperty(prefix + CFG_PRIV_KEY_FILE))
			throw DBPoco::Util::EmptyOptionException(std::string("Missing Configuration Entry: ") + prefix + CFG_PRIV_KEY_FILE);
		
		privateKey = config.getString(prefix + CFG_PRIV_KEY_FILE);
	}
	catch (DBPoco::NullPointerException&)
	{
		throw DBPoco::IllegalStateException(
			"An application configuration is required to obtain the private key passphrase, "
			"but no DBPoco::Util::Application instance is available."
			);
	}
}


} } // namespace DBPoco::Net
