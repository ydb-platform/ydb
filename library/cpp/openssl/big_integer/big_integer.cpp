#include "big_integer.h"

#include <util/generic/yexception.h>
#include <util/generic/scope.h>
#include <util/stream/output.h>

#include <openssl/bn.h>

using namespace NOpenSsl;

TBigInteger::~TBigInteger() noexcept {
    BN_free(Impl_);
}

TBigInteger TBigInteger::FromULong(ui64 value) {
    TBigInteger result(BN_new());

    Y_ENSURE(result.Impl(), "BN_new() failed");
    Y_ENSURE(BN_set_word(result.Impl(), value) == 1, "BN_set_word() failed");

    return result;
}

TBigInteger TBigInteger::FromRegion(const void* ptr, size_t len) {
    auto result = BN_bin2bn((ui8*)(ptr), len, nullptr);

    Y_ENSURE(result, "BN_bin2bn() failed");

    return result;
}

int TBigInteger::Compare(const TBigInteger& a, const TBigInteger& b) noexcept {
    return BN_cmp(a.Impl(), b.Impl());
}

size_t TBigInteger::NumBytes() const noexcept {
    return BN_num_bytes(Impl_);
}

size_t TBigInteger::ToRegion(void* to) const noexcept {
    const auto ret = BN_bn2bin(Impl_, (unsigned char*)to);

    Y_ABORT_UNLESS(ret >= 0, "it happens");

    return ret;
}

TString TBigInteger::ToDecimalString() const {
    auto res = BN_bn2dec(Impl_);

    Y_DEFER {
        OPENSSL_free(res);
    };

    return res;
}

template <>
void Out<TBigInteger>(IOutputStream& out, const TBigInteger& bi) {
    out << bi.ToDecimalString();
}
