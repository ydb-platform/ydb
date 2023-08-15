#pragma once

#include <util/generic/utility.h>
#include <util/generic/noncopyable.h>

struct rsa_st;

namespace NOpenSsl {
    class TBigInteger;

    namespace NRsa {
        class TPublicKey: public TNonCopyable {
        public:
            inline TPublicKey(TPublicKey&& other) noexcept {
                Swap(other);
            }

            TPublicKey(const TBigInteger& e, const TBigInteger& n);
            ~TPublicKey() noexcept;

            size_t OutputLength() const noexcept;

            TBigInteger EncryptNoPad(const TBigInteger& src) const;
            size_t EncryptNoPad(void* dst, const void* src, size_t size) const;

            inline void Swap(TPublicKey& other) noexcept {
                DoSwap(Key_, other.Key_);
            }

        private:
            rsa_st* Key_ = nullptr;
        };
    }
}
