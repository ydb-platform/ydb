//
// Created by Evgeny Sidorov on 12/04/17.
//

#include <library/cpp/digest/argonish/blake2b.h>
#include <library/cpp/digest/argonish/argon2.h>
#include <library/cpp/digest/argonish/internal/proxies/ref/proxy_ref.h>
#if !defined(_arm64_)
#include <library/cpp/digest/argonish/internal/proxies/sse2/proxy_sse2.h>
#include <library/cpp/digest/argonish/internal/proxies/ssse3/proxy_ssse3.h>
#include <library/cpp/digest/argonish/internal/proxies/sse41/proxy_sse41.h>
#include <library/cpp/digest/argonish/internal/proxies/avx2/proxy_avx2.h>
#endif

#include <util/system/cpu_id.h>
#include <util/generic/yexception.h>

namespace NArgonish {
    static EInstructionSet GetBestSet() {
#if !defined(_arm64_)
        if (NX86::HaveAVX2()) {
            return EInstructionSet::AVX2;
        }

        if (NX86::HaveSSE41()) {
            return EInstructionSet::SSE41;
        }

        if (NX86::HaveSSSE3()) {
            return EInstructionSet::SSSE3;
        }

        if (NX86::HaveSSE2()) {
            return EInstructionSet::SSE2;
        }
#endif
        return EInstructionSet::REF;
    }

    TArgon2Factory::TArgon2Factory(bool skipTest) {
        InstructionSet_ = GetBestSet();
        if (!skipTest)
            QuickTest_();
    }

    THolder<IArgon2Base> TArgon2Factory::Create(EInstructionSet instructionSet, EArgon2Type atype, ui32 tcost,
                                                ui32 mcost, ui32 threads, const ui8* key, ui32 keylen) const {
        switch (instructionSet) {
            case EInstructionSet::REF:
                return MakeHolder<TArgon2ProxyREF>(atype, tcost, mcost, threads, key, keylen);
#if !defined(_arm64_)
            case EInstructionSet::SSE2:
                return MakeHolder<TArgon2ProxySSE2>(atype, tcost, mcost, threads, key, keylen);
            case EInstructionSet::SSSE3:
                return MakeHolder<TArgon2ProxySSSE3>(atype, tcost, mcost, threads, key, keylen);
            case EInstructionSet::SSE41:
                return MakeHolder<TArgon2ProxySSSE3>(atype, tcost, mcost, threads, key, keylen);
            case EInstructionSet::AVX2:
                return MakeHolder<TArgon2ProxyAVX2>(atype, tcost, mcost, threads, key, keylen);
#endif
        }

        /* to avoid gcc warning  */
        ythrow yexception() << "Invalid instruction set value";
    }

    THolder<IArgon2Base> TArgon2Factory::Create(EArgon2Type atype, ui32 tcost, ui32 mcost, ui32 threads,
                                                const ui8* key, ui32 keylen) const {
        return Create(InstructionSet_, atype, tcost, mcost, threads, key, keylen);
    }

    EInstructionSet TArgon2Factory::GetInstructionSet() const {
        return InstructionSet_;
    }

    void TArgon2Factory::QuickTest_() const {
        const ui8 password[8] = {'p', 'a', 's', 's', 'w', 'o', 'r', 'd'};
        const ui8 salt[8] = {'s', 'o', 'm', 'e', 's', 'a', 'l', 't'};
        const ui8 test_result[][32] = {
            {0x2e, 0x2e, 0x5e, 0x05, 0xfe, 0x57, 0xac, 0x2c,
             0xf4, 0x72, 0xec, 0xd0, 0x45, 0xef, 0x68, 0x7e,
             0x56, 0x2a, 0x98, 0x0f, 0xd5, 0x03, 0x39, 0xb3,
             0x89, 0xc8, 0x70, 0xe1, 0x96, 0x2b, 0xbc, 0x45},
            {0x95, 0x46, 0x6c, 0xc4, 0xf9, 0x2f, 0x87, 0x49,
             0x54, 0x61, 0x7e, 0xec, 0x0a, 0xa1, 0x19, 0x5d,
             0x22, 0x98, 0x0a, 0xbd, 0x62, 0x5e, 0x5c, 0xac,
             0x44, 0x76, 0x3a, 0xe3, 0xa9, 0xcb, 0x6a, 0xb7},
            {0xc8, 0xe9, 0xae, 0xdc, 0x95, 0x6f, 0x6a, 0x7d,
             0xff, 0x0a, 0x4d, 0x42, 0x94, 0x0d, 0xf6, 0x28,
             0x62, 0x3f, 0x32, 0x8e, 0xa1, 0x23, 0x50, 0x05,
             0xab, 0xac, 0x93, 0x3c, 0x57, 0x09, 0x3e, 0x23}};

        ui8 hash_result[32] = {0};
        for (ui32 atype = (ui32)EArgon2Type::Argon2d; atype <= (ui32)EArgon2Type::Argon2id; ++atype) {
            auto argon2d = MakeHolder<TArgon2ProxyREF>((EArgon2Type)atype, 1U, 1024U, 1U);
            argon2d->Hash(password, sizeof(password), salt, sizeof(salt), hash_result, sizeof(hash_result));
            if (memcmp(test_result[atype], hash_result, sizeof(hash_result)) != 0)
                ythrow yexception() << "Argon2: Runtime test failed for reference implementation";
        }
#if !defined(_arm64_)
        if (InstructionSet_ >= EInstructionSet::SSE2) {
            for (ui32 atype = (ui32)EArgon2Type::Argon2d; atype <= (ui32)EArgon2Type::Argon2id; ++atype) {
                auto argon2d = MakeHolder<TArgon2ProxySSE2>((EArgon2Type)atype, 1U, 1024U, 1U);
                argon2d->Hash(password, sizeof(password), salt, sizeof(salt), hash_result, sizeof(hash_result));
                if (memcmp(test_result[atype], hash_result, sizeof(hash_result)) != 0)
                    ythrow yexception() << "Argon2: Runtime test failed for SSE2 implementation";
            }
        }

        if (InstructionSet_ >= EInstructionSet::SSSE3) {
            for (ui32 atype = (ui32)EArgon2Type::Argon2d; atype <= (ui32)EArgon2Type::Argon2id; ++atype) {
                auto argon2d = MakeHolder<TArgon2ProxySSSE3>((EArgon2Type)atype, 1U, 1024U, 1U);
                argon2d->Hash(password, sizeof(password), salt, sizeof(salt), hash_result, sizeof(hash_result));
                if (memcmp(test_result[atype], hash_result, sizeof(hash_result)) != 0)
                    ythrow yexception() << "Argon2: Runtime test failed for SSSE3 implementation";
            }
        }

        if (InstructionSet_ >= EInstructionSet::SSE41) {
            for (ui32 atype = (ui32)EArgon2Type::Argon2d; atype <= (ui32)EArgon2Type::Argon2id; ++atype) {
                auto argon2d = MakeHolder<TArgon2ProxySSE41>((EArgon2Type)atype, 1U, 1024U, 1U);
                argon2d->Hash(password, sizeof(password), salt, sizeof(salt), hash_result, sizeof(hash_result));
                if (memcmp(test_result[atype], hash_result, sizeof(hash_result)) != 0)
                    ythrow yexception() << "Argon2: Runtime test failed for SSE41 implementation";
            }
        }

        if (InstructionSet_ >= EInstructionSet::AVX2) {
            for (ui32 atype = (ui32)EArgon2Type::Argon2d; atype <= (ui32)EArgon2Type::Argon2id; ++atype) {
                auto argon2d = MakeHolder<TArgon2ProxyAVX2>((EArgon2Type)atype, 1U, 1024U, 1U);
                argon2d->Hash(password, sizeof(password), salt, sizeof(salt), hash_result, sizeof(hash_result));
                if (memcmp(test_result[atype], hash_result, sizeof(hash_result)) != 0)
                    ythrow yexception() << "Argon2: Runtime test failed for AVX2 implementation";
            }
        }
#endif
    }

    TBlake2BFactory::TBlake2BFactory(bool skipTest) {
        InstructionSet_ = GetBestSet();
        if (!skipTest)
            QuickTest_();
    }

    THolder<IBlake2Base> TBlake2BFactory::Create(EInstructionSet instructionSet, size_t outlen, const ui8* key,
                                                 size_t keylen) const {
        switch (instructionSet) {
            case EInstructionSet::REF:
                return MakeHolder<TBlake2BProxyREF>(outlen, key, keylen);
#if !defined(_arm64_)
            case EInstructionSet::SSE2:
                return MakeHolder<TBlake2BProxySSE2>(outlen, key, keylen);
            case EInstructionSet::SSSE3:
                return MakeHolder<TBlake2BProxySSSE3>(outlen, key, keylen);
            case EInstructionSet::SSE41:
                return MakeHolder<TBlake2BProxySSE41>(outlen, key, keylen);
            case EInstructionSet::AVX2:
                return MakeHolder<TBlake2BProxyAVX2>(outlen, key, keylen);
#endif
        }

        /* to supress gcc warning */
        ythrow yexception() << "Invalid instruction set";
    }

    THolder<IBlake2Base> TBlake2BFactory::Create(size_t outlen, const ui8* key, size_t keylen) const {
        return Create(InstructionSet_, outlen, key, keylen);
    }

    EInstructionSet TBlake2BFactory::GetInstructionSet() const {
        return InstructionSet_;
    }

    void TBlake2BFactory::QuickTest_() const {
        const char* test_str = "abc";
        const ui8 test_result[] = {
            0xcf, 0x4a, 0xb7, 0x91, 0xc6, 0x2b, 0x8d, 0x2b,
            0x21, 0x09, 0xc9, 0x02, 0x75, 0x28, 0x78, 0x16};

        ui8 hash_val[16];
        if (InstructionSet_ >= EInstructionSet::REF) {
            auto blake2 = MakeHolder<TBlake2BProxyREF>(16U);
            blake2->Update(test_str, 3);
            blake2->Final(hash_val, 16);
            if (memcmp(test_result, hash_val, 16) != 0)
                ythrow yexception() << "Blake2B: Runtime test failed for reference implementation";
        }
#if !defined(_arm64_)
        if (InstructionSet_ >= EInstructionSet::SSE2) {
            auto blake2 = MakeHolder<TBlake2BProxySSE2>(16U);
            blake2->Update(test_str, 3);
            blake2->Final(hash_val, 16);
            if (memcmp(test_result, hash_val, 16) != 0)
                ythrow yexception() << "Blake2B: Runtime test failed for SSE2 implementation";
        }

        if (InstructionSet_ >= EInstructionSet::SSSE3) {
            auto blake2 = MakeHolder<TBlake2BProxySSSE3>(16U);
            blake2->Update(test_str, 3);
            blake2->Final(hash_val, 16);
            if (memcmp(test_result, hash_val, 16) != 0)
                ythrow yexception() << "Blake2B: Runtime test failed for SSSE3 implementation";
        }

        if (InstructionSet_ >= EInstructionSet::SSE41) {
            auto blake2 = MakeHolder<TBlake2BProxySSE41>(16U);
            blake2->Update(test_str, 3);
            blake2->Final(hash_val, 16);
            if (memcmp(test_result, hash_val, 16) != 0)
                ythrow yexception() << "Blake2B: Runtime test failed for SSE41 implmenetation";
        }

        if (InstructionSet_ >= EInstructionSet::AVX2) {
            auto blake2 = MakeHolder<TBlake2BProxyAVX2>(16U);
            blake2->Update(test_str, 3);
            blake2->Final(hash_val, 16);
            if (memcmp(test_result, hash_val, 16) != 0)
                ythrow yexception() << "Blake2B: Runtime test failed for AVX2 implementation";
        }
#endif
    }
}
