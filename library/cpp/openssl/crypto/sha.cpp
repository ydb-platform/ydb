#include "sha.h"

#include <util/generic/yexception.h>

#include <openssl/sha.h>

namespace NOpenSsl {
    namespace NSha1 {
        static_assert(DIGEST_LENGTH == SHA_DIGEST_LENGTH);

        TDigest Calc(const void* data, size_t dataSize) {
            TDigest digest;
            Y_ENSURE(SHA1(static_cast<const ui8*>(data), dataSize, digest.data()) != nullptr);
            return digest;
        }

        TCalcer::TCalcer()
            : Context{new SHAstate_st} {
            Y_ENSURE(SHA1_Init(Context.Get()) == 1);
        }

        TCalcer::~TCalcer() {
        }

        void TCalcer::Update(const void* data, size_t dataSize) {
            Y_ENSURE(SHA1_Update(Context.Get(), data, dataSize) == 1);
        }

        TDigest TCalcer::Final() {
            TDigest digest;
            Y_ENSURE(SHA1_Final(digest.data(), Context.Get()) == 1);
            return digest;
        }
    }
    namespace NSha256 {
        static_assert(DIGEST_LENGTH == SHA256_DIGEST_LENGTH);

        TDigest Calc(const void* data, size_t dataSize) {
            TDigest digest;
            Y_ENSURE(SHA256(static_cast<const ui8*>(data), dataSize, digest.data()) != nullptr);
            return digest;
        }

        TCalcer::TCalcer()
            : Context{new SHA256state_st} {
            Y_ENSURE(SHA256_Init(Context.Get()) == 1);
        }

        TCalcer::~TCalcer() {
        }

        void TCalcer::Update(const void* data, size_t dataSize) {
            Y_ENSURE(SHA256_Update(Context.Get(), data, dataSize) == 1);
        }

        TDigest TCalcer::Final() {
            TDigest digest;
            Y_ENSURE(SHA256_Final(digest.data(), Context.Get()) == 1);
            return digest;
        }
    }
    namespace NSha224 {
        static_assert(DIGEST_LENGTH == SHA224_DIGEST_LENGTH);

        TDigest Calc(const void* data, size_t dataSize) {
            TDigest digest;
            Y_ENSURE(SHA224(static_cast<const ui8*>(data), dataSize, digest.data()) != nullptr);
            return digest;
        }

        TCalcer::TCalcer()
            : Context{new SHA256state_st} {
            Y_ENSURE(SHA224_Init(Context.Get()) == 1);
        }

        TCalcer::~TCalcer() {
        }

        void TCalcer::Update(const void* data, size_t dataSize) {
            Y_ENSURE(SHA224_Update(Context.Get(), data, dataSize) == 1);
        }

        TDigest TCalcer::Final() {
            TDigest digest;
            Y_ENSURE(SHA224_Final(digest.data(), Context.Get()) == 1);
            return digest;
        }
    }
}
