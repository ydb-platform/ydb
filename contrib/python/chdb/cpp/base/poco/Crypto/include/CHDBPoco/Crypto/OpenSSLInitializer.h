//
// OpenSSLInitializer.h
//
// Library: Crypto
// Package: CryptoCore
// Module:  OpenSSLInitializer
//
// Definition of the OpenSSLInitializer class.
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Crypto_OpenSSLInitializer_INCLUDED
#define CHDB_Crypto_OpenSSLInitializer_INCLUDED


#include <openssl/crypto.h>
#include "CHDBPoco/AtomicCounter.h"
#include "CHDBPoco/Crypto/Crypto.h"
#include "CHDBPoco/Mutex.h"

#if defined(OPENSSL_FIPS) && OPENSSL_VERSION_NUMBER < 0x010001000L
#    error #include <openssl/fips.h>
#endif


extern "C" {
struct CRYPTO_dynlock_value
{
    CHDBPoco::FastMutex _mutex;
};
}


namespace CHDBPoco
{
namespace Crypto
{


    class Crypto_API OpenSSLInitializer
    /// Initializes the OpenSSL library.
    ///
    /// The class ensures the earliest initialization and the
    /// latest shutdown of the OpenSSL library.
    {
    public:
        OpenSSLInitializer();
        /// Automatically initialize OpenSSL on startup.

        ~OpenSSLInitializer();
        /// Automatically shut down OpenSSL on exit.

        static void initialize();
        /// Initializes the OpenSSL machinery.

        static void uninitialize();
        /// Shuts down the OpenSSL machinery.

        static bool isFIPSEnabled();
        // Returns true if FIPS mode is enabled, false otherwise.

        static void enableFIPSMode(bool enabled);
        // Enable or disable FIPS mode. If FIPS is not available, this method doesn't do anything.

    protected:
        enum
        {
            SEEDSIZE = 256
        };

        // OpenSSL multithreading support
        static void lock(int mode, int n, const char * file, int line);
        static unsigned long id();
        static struct CRYPTO_dynlock_value * dynlockCreate(const char * file, int line);
        static void dynlock(int mode, struct CRYPTO_dynlock_value * lock, const char * file, int line);
        static void dynlockDestroy(struct CRYPTO_dynlock_value * lock, const char * file, int line);

    private:
        static CHDBPoco::FastMutex * _mutexes;
        static CHDBPoco::AtomicCounter _rc;
    };


    //
    // inlines
    //
    inline bool OpenSSLInitializer::isFIPSEnabled()
    {
#ifdef OPENSSL_FIPS
        return FIPS_mode() ? true : false;
#else
        return false;
#endif
    }

#ifdef OPENSSL_FIPS
    inline void OpenSSLInitializer::enableFIPSMode(bool enabled)
    {
        FIPS_mode_set(enabled);
    }
#else
    inline void OpenSSLInitializer::enableFIPSMode(bool /*enabled*/)
    {
    }
#endif


}
} // namespace CHDBPoco::Crypto


#endif // CHDB_Crypto_OpenSSLInitializer_INCLUDED
