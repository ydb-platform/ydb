//
// OpenSSLInitializer.cpp
//
// Library: Crypto
// Package: CryptoCore
// Module:  OpenSSLInitializer
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Crypto/OpenSSLInitializer.h"
#include "DBPoco/RandomStream.h"
#include "DBPoco/Thread.h"
#include <openssl/ssl.h>
#include <openssl/rand.h>
#include <openssl/crypto.h>
#include <openssl/err.h>
#if OPENSSL_VERSION_NUMBER >= 0x0907000L
#include <openssl/conf.h>
#endif


using DBPoco::RandomInputStream;
using DBPoco::Thread;




namespace DBPoco {
namespace Crypto {


DBPoco::FastMutex* OpenSSLInitializer::_mutexes(0);
DBPoco::AtomicCounter OpenSSLInitializer::_rc;


OpenSSLInitializer::OpenSSLInitializer()
{
	initialize();
}


OpenSSLInitializer::~OpenSSLInitializer()
{
	try
	{
		uninitialize();
	}
	catch (...)
	{
		DB_poco_unexpected();
	}
}


void OpenSSLInitializer::initialize()
{
	if (++_rc == 1)
	{
#if OPENSSL_VERSION_NUMBER >= 0x0907000L
		OPENSSL_config(NULL);
#endif
		SSL_library_init();
		SSL_load_error_strings();
		OpenSSL_add_all_algorithms();
		
		char seed[SEEDSIZE];
		RandomInputStream rnd;
		rnd.read(seed, sizeof(seed));
		RAND_seed(seed, SEEDSIZE);
		
		int nMutexes = CRYPTO_num_locks();
		_mutexes = new DBPoco::FastMutex[nMutexes];
		CRYPTO_set_locking_callback(&OpenSSLInitializer::lock);
// Not needed on Windows (see SF #110: random unhandled exceptions when linking with ssl).
// https://sourceforge.net/p/poco/bugs/110/
//
// From http://www.openssl.org/docs/crypto/threads.html :
// "If the application does not register such a callback using CRYPTO_THREADID_set_callback(), 
//  then a default implementation is used - on Windows and BeOS this uses the system's 
//  default thread identifying APIs"
		CRYPTO_set_id_callback(&OpenSSLInitializer::id);
		CRYPTO_set_dynlock_create_callback(&OpenSSLInitializer::dynlockCreate);
		CRYPTO_set_dynlock_lock_callback(&OpenSSLInitializer::dynlock);
		CRYPTO_set_dynlock_destroy_callback(&OpenSSLInitializer::dynlockDestroy);
	}
}


void OpenSSLInitializer::uninitialize()
{
	if (--_rc == 0)
	{
		EVP_cleanup();
		ERR_free_strings();
		CRYPTO_set_locking_callback(0);
		CRYPTO_set_id_callback(0);
		delete [] _mutexes;
		
		CONF_modules_free();
	}
}


void OpenSSLInitializer::lock(int mode, int n, const char* file, int line)
{
	if (mode & CRYPTO_LOCK)
		_mutexes[n].lock();
	else
		_mutexes[n].unlock();
}


unsigned long OpenSSLInitializer::id()
{
	// Note: we use an old-style C cast here because
	// neither static_cast<> nor reinterpret_cast<>
	// work uniformly across all platforms.
	return (unsigned long) DBPoco::Thread::currentTid();
}


struct CRYPTO_dynlock_value* OpenSSLInitializer::dynlockCreate(const char* file, int line)
{
	return new CRYPTO_dynlock_value;
}


void OpenSSLInitializer::dynlock(int mode, struct CRYPTO_dynlock_value* lock, const char* file, int line)
{
	DB_poco_check_ptr (lock);

	if (mode & CRYPTO_LOCK)
		lock->_mutex.lock();
	else
		lock->_mutex.unlock();
}


void OpenSSLInitializer::dynlockDestroy(struct CRYPTO_dynlock_value* lock, const char* file, int line)
{
	delete lock;
}


void initializeCrypto()
{
	OpenSSLInitializer::initialize();
}


void uninitializeCrypto()
{
	OpenSSLInitializer::uninitialize();
}


} } // namespace DBPoco::Crypto
