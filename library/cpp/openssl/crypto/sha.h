#pragma once

#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>
#include <util/system/types.h>

#include <array>

struct SHAstate_st;
struct SHA256state_st;

namespace NOpenSsl::NSha1 {
    constexpr size_t DIGEST_LENGTH = 20;
    using TDigest = std::array<ui8, DIGEST_LENGTH>;

    // not fragmented input
    TDigest Calc(const void* data, size_t dataSize);

    inline TDigest Calc(TStringBuf s) {
        return Calc(s.data(), s.length());
    }

    // fragmented input
    class TCalcer {
    public:
        TCalcer();
        ~TCalcer();
        void Update(const void* data, size_t dataSize);

        void Update(TStringBuf s) {
            Update(s.data(), s.length());
        }

        template <typename T>
        void UpdateWithPodValue(const T& value) {
            Update(&value, sizeof(value));
        }

        TDigest Final();

    private:
        THolder<SHAstate_st> Context;
    };
}

namespace NOpenSsl::NSha256 {
    constexpr size_t DIGEST_LENGTH = 32;
    using TDigest = std::array<ui8, DIGEST_LENGTH>;

    // not fragmented input
    TDigest Calc(const void* data, size_t dataSize);

    inline TDigest Calc(TStringBuf s) {
        return Calc(s.data(), s.length());
    }

    // fragmented input
    class TCalcer {
    public:
        TCalcer();
        ~TCalcer();
        void Update(const void* data, size_t dataSize);

        void Update(TStringBuf s) {
            Update(s.data(), s.length());
        }

        template <typename T>
        void UpdateWithPodValue(const T& value) {
            Update(&value, sizeof(value));
        }

        TDigest Final();

    private:
        THolder<SHA256state_st> Context;
    };
}

namespace NOpenSsl::NSha224 {
    constexpr size_t DIGEST_LENGTH = 28;
    using TDigest = std::array<ui8, DIGEST_LENGTH>;

    // not fragmented input
    TDigest Calc(const void* data, size_t dataSize);

    inline TDigest Calc(TStringBuf s) {
        return Calc(s.data(), s.length());
    }

    // fragmented input
    class TCalcer {
    public:
        TCalcer();
        ~TCalcer();
        void Update(const void* data, size_t dataSize);

        void Update(TStringBuf s) {
            Update(s.data(), s.length());
        }

        template <typename T>
        void UpdateWithPodValue(const T& value) {
            Update(&value, sizeof(value));
        }

        TDigest Final();

    private:
        THolder<SHA256state_st> Context;
    };
}
