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


#include "CHDBPoco/Net/KeyFileHandler.h"
#include "CHDBPoco/Net/SSLManager.h"
#include "CHDBPoco/File.h"
#include "CHDBPoco/Util/AbstractConfiguration.h"
#include "CHDBPoco/Util/Application.h"
#include "CHDBPoco/Util/OptionException.h"


namespace CHDBPoco {
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
		CHDBPoco::Util::AbstractConfiguration& config = CHDBPoco::Util::Application::instance().config();
		std::string prefix = serverSide() ? SSLManager::CFG_SERVER_PREFIX : SSLManager::CFG_CLIENT_PREFIX;
		if (!config.hasProperty(prefix + CFG_PRIV_KEY_FILE))
			throw CHDBPoco::Util::EmptyOptionException(std::string("Missing Configuration Entry: ") + prefix + CFG_PRIV_KEY_FILE);
		
		privateKey = config.getString(prefix + CFG_PRIV_KEY_FILE);
	}
	catch (CHDBPoco::NullPointerException&)
	{
		throw CHDBPoco::IllegalStateException(
			"An application configuration is required to obtain the private key passphrase, "
			"but no CHDBPoco::Util::Application instance is available."
			);
	}
}


} } // namespace CHDBPoco::Net
