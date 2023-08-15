#include "rsa.h"

#include <library/cpp/openssl/big_integer/big_integer.h>
#include <library/cpp/openssl/init/init.h>

#include <util/generic/yexception.h>
#include <util/generic/buffer.h>

#include <openssl/bn.h>
#include <openssl/rsa.h>

using namespace NOpenSsl;
using namespace NOpenSsl::NRsa;

namespace {
    struct TInit {
        inline TInit() {
            InitOpenSSL();
        }
    } INIT;
}

TPublicKey::TPublicKey(const TBigInteger& e, const TBigInteger& n)
    : Key_(RSA_new())
{
    Y_ENSURE(Key_, "RSA_new() failed");

    RSA_set0_key(Key_, BN_dup(n.Impl()), BN_dup(e.Impl()), nullptr);
}

TPublicKey::~TPublicKey() noexcept {
    RSA_free(Key_);
}

size_t TPublicKey::OutputLength() const noexcept {
    return RSA_size(Key_);
}

size_t TPublicKey::EncryptNoPad(void* dst, const void* src, size_t size) const {
    auto len = RSA_public_encrypt(size, (const ui8*)src, (ui8*)dst, Key_, RSA_NO_PADDING);

    Y_ENSURE(len >= 0, "RSA_public_encrypt() failed");

    return len;
}

TBigInteger TPublicKey::EncryptNoPad(const TBigInteger& src) const {
    const auto len1 = OutputLength();
    const auto len2 = src.NumBytes();
    TBuffer buf(len1 + len2);

    char* buf1 = (char*)buf.Data();
    char* buf2 = buf1 + len1;

    return TBigInteger::FromRegion(buf1, EncryptNoPad(buf1, buf2, src.ToRegion(buf2)));
}
