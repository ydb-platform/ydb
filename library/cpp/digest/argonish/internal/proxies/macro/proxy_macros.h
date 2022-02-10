#pragma once

//
// Created by Evgeny Sidorov on 12/04/17.
//
/**
 * ZEROUPPER macro is only used for AVX2 instruction set to clear up the upper half of YMM registers
 * It's done to avoid performance penalty when CPU switches to non-AVX2 code (according to Agner)
 * and the post at https://software.intel.com/en-us/articles/intel-avx-state-transitions-migrating-sse-code-to-avx
 */

#define ARGON2_PROXY_CLASS_DECL(IS)                                                                                   \
    class TArgon2Proxy##IS final: public IArgon2Base {                                                                \
    public:                                                                                                           \
        TArgon2Proxy##IS(EArgon2Type atype, ui32 tcost, ui32 mcost, ui32 threads,                                     \
                         const ui8* key = nullptr, ui32 keylen = 0);                                                  \
        virtual ~TArgon2Proxy##IS();                                                                                  \
                                                                                                                      \
        virtual void Hash(const ui8* pwd, ui32 pwdlen, const ui8* salt, ui32 saltlen,                                 \
                          ui8* out, ui32 outlen, const ui8* aad = nullptr, ui32 aadlen = 0) const override;           \
        virtual bool Verify(const ui8* pwd, ui32 pwdlen, const ui8* salt, ui32 saltlen,                               \
                            const ui8* hash, ui32 hashlen, const ui8* aad = nullptr, ui32 aadlen = 0) const override; \
        virtual void HashWithCustomMemory(ui8* memory, size_t mlen, const ui8* pwd, ui32 pwdlen,                      \
                                          const ui8* salt, ui32 saltlen, ui8* out, ui32 outlen,                       \
                                          const ui8* aad = nullptr, ui32 aadlen = 0) const override;                  \
        virtual bool VerifyWithCustomMemory(ui8* memory, size_t mlen, const ui8* pwd, ui32 pwdlen,                    \
                                            const ui8* salt, ui32 saltlen, const ui8* hash, ui32 hashlen,             \
                                            const ui8* aad = nullptr, ui32 aadlen = 0) const override;                \
        virtual size_t GetMemorySize() const override;                                                                \
                                                                                                                      \
    protected:                                                                                                        \
        THolder<IArgon2Base> argon2;                                                                                  \
    };

#define ARGON2_INSTANCE_DECL(IS_val, mcost_val, threads_val)                                     \
    if (mcost == mcost_val && threads == threads_val) {                                          \
        argon2 = MakeHolder<TArgon2##IS_val<mcost_val, threads_val>>(atype, tcost, key, keylen); \
        return;                                                                                  \
    }

#define ARGON2_PROXY_CLASS_IMPL(IS)                                                                                           \
    TArgon2Proxy##IS::TArgon2Proxy##IS(EArgon2Type atype, ui32 tcost, ui32 mcost, ui32 threads,                               \
                                       const ui8* key, ui32 keylen) {                                                         \
        if ((key == nullptr && keylen > 0) || keylen > ARGON2_SECRET_MAX_LENGTH)                                              \
            ythrow yexception() << "key is null or keylen equals 0 or key is too long";                                       \
                                                                                                                              \
        ARGON2_INSTANCE_DECL(IS, 1, 1)                                                                                        \
        ARGON2_INSTANCE_DECL(IS, 8, 1)                                                                                        \
        ARGON2_INSTANCE_DECL(IS, 16, 1)                                                                                       \
        ARGON2_INSTANCE_DECL(IS, 32, 1)                                                                                       \
        ARGON2_INSTANCE_DECL(IS, 64, 1)                                                                                       \
        ARGON2_INSTANCE_DECL(IS, 128, 1)                                                                                       \
        ARGON2_INSTANCE_DECL(IS, 256, 1)                                                                                       \
        ARGON2_INSTANCE_DECL(IS, 512, 1)                                                                                      \
        ARGON2_INSTANCE_DECL(IS, 1024, 1)                                                                                     \
        ARGON2_INSTANCE_DECL(IS, 2048, 1)                                                                                     \
        ARGON2_INSTANCE_DECL(IS, 4096, 1)                                                                                     \
        ARGON2_INSTANCE_DECL(IS, 8192, 1)                                                                                     \
        ARGON2_INSTANCE_DECL(IS, 16384, 1)                                                                                    \
        ARGON2_INSTANCE_DECL(IS, 32768, 1)                                                                                    \
        ARGON2_INSTANCE_DECL(IS, 65536, 1)                                                                                    \
        ARGON2_INSTANCE_DECL(IS, 131072, 1)                                                                                   \
        ARGON2_INSTANCE_DECL(IS, 262144, 1)                                                                                   \
        ARGON2_INSTANCE_DECL(IS, 524288, 1)                                                                                   \
        ARGON2_INSTANCE_DECL(IS, 1048576, 1)                                                                                  \
        ARGON2_INSTANCE_DECL(IS, 1, 2)                                                                                        \
        ARGON2_INSTANCE_DECL(IS, 32, 2)                                                                                       \
        ARGON2_INSTANCE_DECL(IS, 64, 2)                                                                                       \
        ARGON2_INSTANCE_DECL(IS, 512, 2)                                                                                      \
        ARGON2_INSTANCE_DECL(IS, 1024, 2)                                                                                     \
        ARGON2_INSTANCE_DECL(IS, 2048, 2)                                                                                     \
        ARGON2_INSTANCE_DECL(IS, 4096, 2)                                                                                     \
        ARGON2_INSTANCE_DECL(IS, 8192, 2)                                                                                     \
        ARGON2_INSTANCE_DECL(IS, 16384, 2)                                                                                    \
        ARGON2_INSTANCE_DECL(IS, 32768, 2)                                                                                    \
        ARGON2_INSTANCE_DECL(IS, 65536, 2)                                                                                    \
        ARGON2_INSTANCE_DECL(IS, 131072, 2)                                                                                   \
        ARGON2_INSTANCE_DECL(IS, 262144, 2)                                                                                   \
        ARGON2_INSTANCE_DECL(IS, 524288, 2)                                                                                   \
        ARGON2_INSTANCE_DECL(IS, 1048576, 2)                                                                                  \
        ARGON2_INSTANCE_DECL(IS, 1, 4)                                                                                        \
        ARGON2_INSTANCE_DECL(IS, 32, 4)                                                                                       \
        ARGON2_INSTANCE_DECL(IS, 64, 4)                                                                                       \
        ARGON2_INSTANCE_DECL(IS, 512, 4)                                                                                      \
        ARGON2_INSTANCE_DECL(IS, 1024, 4)                                                                                     \
        ARGON2_INSTANCE_DECL(IS, 2048, 4)                                                                                     \
        ARGON2_INSTANCE_DECL(IS, 4096, 4)                                                                                     \
        ARGON2_INSTANCE_DECL(IS, 8192, 4)                                                                                     \
        ARGON2_INSTANCE_DECL(IS, 16384, 4)                                                                                    \
        ARGON2_INSTANCE_DECL(IS, 32768, 4)                                                                                    \
        ARGON2_INSTANCE_DECL(IS, 65536, 4)                                                                                    \
        ARGON2_INSTANCE_DECL(IS, 131072, 4)                                                                                   \
        ARGON2_INSTANCE_DECL(IS, 262144, 4)                                                                                   \
        ARGON2_INSTANCE_DECL(IS, 524288, 4)                                                                                   \
        ARGON2_INSTANCE_DECL(IS, 1048576, 4)                                                                                  \
                                                                                                                              \
        ythrow yexception() << "These parameters are not supported. Please add the corresponding ARGON2_INSTANCE_DECL macro"; \
    }                                                                                                                         \
                                                                                                                              \
    TArgon2Proxy##IS::~TArgon2Proxy##IS() {                                                                                   \
    }                                                                                                                         \
                                                                                                                              \
    void TArgon2Proxy##IS::Hash(const ui8* pwd, ui32 pwdlen, const ui8* salt, ui32 saltlen,                                   \
                                ui8* out, ui32 outlen, const ui8* aad, ui32 aadlen) const {                                   \
        if (saltlen < ARGON2_SALT_MIN_LEN)                                                                                    \
            ythrow yexception() << "salt is too short";                                                                       \
        if (outlen < ARGON2_MIN_OUTLEN)                                                                                       \
            ythrow yexception() << "output length is too short";                                                              \
                                                                                                                              \
        argon2->Hash(pwd, pwdlen, salt, saltlen, out, outlen, aad, aadlen);                                                   \
        ZEROUPPER                                                                                                             \
    }                                                                                                                         \
                                                                                                                              \
    bool TArgon2Proxy##IS::Verify(const ui8* pwd, ui32 pwdlen, const ui8* salt, ui32 saltlen,                                 \
                                  const ui8* hash, ui32 hashlen, const ui8* aad, ui32 aadlen) const {                         \
        if (saltlen < ARGON2_SALT_MIN_LEN)                                                                                    \
            ythrow yexception() << "salt is too short";                                                                       \
        if (hashlen < ARGON2_MIN_OUTLEN)                                                                                      \
            ythrow yexception() << "hash length is too short";                                                                \
                                                                                                                              \
        return argon2->Verify(pwd, pwdlen, salt, saltlen, hash, hashlen, aad, aadlen);                                        \
        ZEROUPPER                                                                                                             \
    }                                                                                                                         \
                                                                                                                              \
    void TArgon2Proxy##IS::HashWithCustomMemory(ui8* memory, size_t mlen, const ui8* pwd, ui32 pwdlen,                        \
                                                const ui8* salt, ui32 saltlen, ui8* out, ui32 outlen,                         \
                                                const ui8* aad, ui32 aadlen) const {                                          \
        if (saltlen < ARGON2_SALT_MIN_LEN)                                                                                    \
            ythrow yexception() << "salt is too short";                                                                       \
        if (outlen < ARGON2_MIN_OUTLEN)                                                                                       \
            ythrow yexception() << "output length is too short";                                                              \
                                                                                                                              \
        argon2->HashWithCustomMemory(memory, mlen, pwd, pwdlen, salt, saltlen, out, outlen, aad, aadlen);                     \
        ZEROUPPER                                                                                                             \
    }                                                                                                                         \
                                                                                                                              \
    bool TArgon2Proxy##IS::VerifyWithCustomMemory(ui8* memory, size_t mlen, const ui8* pwd, ui32 pwdlen,                      \
                                                  const ui8* salt, ui32 saltlen, const ui8* hash, ui32 hashlen,               \
                                                  const ui8* aad, ui32 aadlen) const {                                        \
        if (saltlen < ARGON2_SALT_MIN_LEN)                                                                                    \
            ythrow yexception() << "salt is too short";                                                                       \
        if (hashlen < ARGON2_MIN_OUTLEN)                                                                                      \
            ythrow yexception() << "hash length is too short";                                                                \
                                                                                                                              \
        return argon2->VerifyWithCustomMemory(memory, mlen, pwd, pwdlen, salt, saltlen, hash, hashlen, aad, aadlen);          \
        ZEROUPPER                                                                                                             \
    }                                                                                                                         \
                                                                                                                              \
    size_t TArgon2Proxy##IS::GetMemorySize() const {                                                                          \
        return argon2->GetMemorySize();                                                                                       \
    }

#define BLAKE2B_PROXY_CLASS_DECL(IS)                                                    \
    class TBlake2BProxy##IS final: public IBlake2Base {                                 \
    public:                                                                             \
        TBlake2BProxy##IS(size_t outlen, const void* key = nullptr, size_t keylen = 0); \
        virtual void Update(ui32 in) override;                                          \
        virtual void Update(const void* pin, size_t inlen) override;                    \
        virtual void Final(void* out, size_t outlen) override;                          \
                                                                                        \
    protected:                                                                          \
        THolder<IBlake2Base> blake2;                                                    \
    };

#define BLAKE2B_PROXY_CLASS_IMPL(IS)                                                      \
    TBlake2BProxy##IS::TBlake2BProxy##IS(size_t outlen, const void* key, size_t keylen) { \
        if (!outlen || outlen > BLAKE2B_OUTBYTES)                                         \
            ythrow yexception() << "outlen equals 0 or too long";                         \
                                                                                          \
        if (key == nullptr) {                                                             \
            blake2 = MakeHolder<TBlake2B<EInstructionSet::IS>>(outlen);                   \
            return;                                                                       \
        }                                                                                 \
                                                                                          \
        if (!key || !keylen || keylen > BLAKE2B_KEYBYTES)                                 \
            ythrow yexception() << "key is null or too long";                             \
                                                                                          \
        blake2 = MakeHolder<TBlake2B<EInstructionSet::IS>>(outlen, key, keylen);          \
    }                                                                                     \
                                                                                          \
    void TBlake2BProxy##IS::Update(ui32 in) {                                             \
        blake2->Update(in);                                                               \
        ZEROUPPER                                                                         \
    }                                                                                     \
                                                                                          \
    void TBlake2BProxy##IS::Update(const void* pin, size_t inlen) {                       \
        blake2->Update(pin, inlen);                                                       \
        ZEROUPPER                                                                         \
    }                                                                                     \
                                                                                          \
    void TBlake2BProxy##IS::Final(void* out, size_t outlen) {                             \
        blake2->Final(out, outlen);                                                       \
        ZEROUPPER                                                                         \
    }
