#include <library/cpp/digest/argonish/argon2.h>
#include <library/cpp/digest/argonish/blake2b.h>
#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(ArgonishTest) {
    const ui8 GenKatPassword[32] = {
        0x01,
        0x01,
        0x01,
        0x01,
        0x01,
        0x01,
        0x01,
        0x01,
        0x01,
        0x01,
        0x01,
        0x01,
        0x01,
        0x01,
        0x01,
        0x01,
        0x01,
        0x01,
        0x01,
        0x01,
        0x01,
        0x01,
        0x01,
        0x01,
        0x01,
        0x01,
        0x01,
        0x01,
        0x01,
        0x01,
        0x01,
        0x01,
    };

    const ui8 GenKatSalt[16] = {
        0x02,
        0x02,
        0x02,
        0x02,
        0x02,
        0x02,
        0x02,
        0x02,
        0x02,
        0x02,
        0x02,
        0x02,
        0x02,
        0x02,
        0x02,
        0x02,
    };

    const ui8 GenKatSecret[8] = {
        0x03,
        0x03,
        0x03,
        0x03,
        0x03,
        0x03,
        0x03,
        0x03,
    };

    const ui8 GenKatAAD[12] = {
        0x04,
        0x04,
        0x04,
        0x04,
        0x04,
        0x04,
        0x04,
        0x04,
        0x04,
        0x04,
        0x04,
        0x04,
    };

    constexpr NArgonish::EInstructionSet MaxArch =
#if !defined(_arm64_)
        NArgonish::EInstructionSet::AVX2
#else
        NArgonish::EInstructionSet::REF
#endif
    ;

    Y_UNIT_TEST(Argon2_Factory_SelfTest) {
        try {
            NArgonish::TArgon2Factory factory;
        } catch (...) {
            UNIT_FAIL("Argon2 factory self-test fail");
        }
    }

    Y_UNIT_TEST(Argon2d) {
        const ui8 TResult[32] = {
            0x7b, 0xa5, 0xa1, 0x7a, 0x72, 0xf7, 0xe5, 0x99,
            0x77, 0xf7, 0xf2, 0x3d, 0x10, 0xe6, 0x21, 0x89,
            0x8c, 0x63, 0xce, 0xbe, 0xed, 0xda, 0xbd, 0x15,
            0xd8, 0xc6, 0x8f, 0x53, 0xea, 0xb2, 0x1a, 0x32};

        NArgonish::TArgon2Factory factory;
        for (int i = (int)NArgonish::EInstructionSet::REF; i <= (int)MaxArch; ++i) {
            ui8 result[32];
            auto argon2d = factory.Create(
                (NArgonish::EInstructionSet)i, NArgonish::EArgon2Type::Argon2d,
                1, 32, 1, GenKatSecret, sizeof(GenKatSecret));

            argon2d->Hash(GenKatPassword, sizeof(GenKatPassword), GenKatSalt, sizeof(GenKatSalt),
                          result, sizeof(result), GenKatAAD, sizeof(GenKatAAD));

            UNIT_ASSERT(memcmp(result, TResult, sizeof(result)) == 0);

            UNIT_ASSERT(argon2d->Verify(GenKatPassword, sizeof(GenKatPassword),
                                        GenKatSalt, sizeof(GenKatSalt),
                                        TResult, sizeof(TResult),
                                        GenKatAAD, sizeof(GenKatAAD)));
        }
    }

    Y_UNIT_TEST(Argon2i) {
        const ui8 TResult[32] = {
            0x87, 0x4d, 0x23, 0xfb, 0x9f, 0x55, 0xe2, 0xff,
            0x66, 0xbc, 0x19, 0x03, 0x46, 0xe7, 0x01, 0x19,
            0x7c, 0x9f, 0x25, 0xd1, 0x1d, 0xa4, 0x5a, 0xad,
            0x0d, 0x5d, 0x24, 0x19, 0x8a, 0xac, 0xd2, 0xbb};

        NArgonish::TArgon2Factory factory;
        for (int i = (int)NArgonish::EInstructionSet::REF; i <= (int)MaxArch; ++i) {
            ui8 result[32];
            auto argon2i = factory.Create(
                (NArgonish::EInstructionSet)i, NArgonish::EArgon2Type::Argon2i,
                1, 32, 1, GenKatSecret, sizeof(GenKatSecret));

            argon2i->Hash(GenKatPassword, sizeof(GenKatPassword), GenKatSalt, sizeof(GenKatSalt),
                          result, sizeof(result), GenKatAAD, sizeof(GenKatAAD));

            UNIT_ASSERT(memcmp(result, TResult, sizeof(result)) == 0);

            UNIT_ASSERT(argon2i->Verify(GenKatPassword, sizeof(GenKatPassword),
                                        GenKatSalt, sizeof(GenKatSalt),
                                        TResult, sizeof(TResult),
                                        GenKatAAD, sizeof(GenKatAAD)));
        }
    }

    Y_UNIT_TEST(Argon2id) {
        const ui8 TResult[32] = {
            0x99, 0xdf, 0xcf, 0xc2, 0x89, 0x76, 0x93, 0x9d,
            0xa2, 0x97, 0x09, 0x44, 0x34, 0xd8, 0x6f, 0xd0,
            0x0c, 0x94, 0x9a, 0x0f, 0x31, 0x8c, 0x22, 0xf0,
            0xcb, 0xb4, 0x69, 0xaa, 0xa8, 0x72, 0x18, 0xba};

        NArgonish::TArgon2Factory factory;
        for (int i = (int)NArgonish::EInstructionSet::REF; i <= (int)MaxArch; ++i) {
            ui8 result[32];
            auto argon2id = factory.Create(
                (NArgonish::EInstructionSet)i, NArgonish::EArgon2Type::Argon2id,
                1, 32, 1, GenKatSecret, sizeof(GenKatSecret));

            argon2id->Hash(GenKatPassword, sizeof(GenKatPassword), GenKatSalt, sizeof(GenKatSalt),
                           result, sizeof(result), GenKatAAD, sizeof(GenKatAAD));

            UNIT_ASSERT(memcmp(result, TResult, sizeof(result)) == 0);

            UNIT_ASSERT(argon2id->Verify(GenKatPassword, sizeof(GenKatPassword),
                                         GenKatSalt, sizeof(GenKatSalt),
                                         TResult, sizeof(TResult),
                                         GenKatAAD, sizeof(GenKatAAD)));
        }
    }

    Y_UNIT_TEST(Argon2d_2p) {
        const ui8 TResult[32] = {
            0x59, 0xb0, 0x94, 0x62, 0xcf, 0xdc, 0xd2, 0xb4,
            0x0a, 0xbd, 0x17, 0x81, 0x0a, 0x47, 0x4a, 0x8e,
            0xc1, 0xab, 0xb7, 0xc1, 0x8d, 0x07, 0x53, 0x7c,
            0xb9, 0x64, 0xa2, 0x59, 0x3f, 0xe9, 0xd9, 0xc5};

        NArgonish::TArgon2Factory factory;
        for (int i = (int)NArgonish::EInstructionSet::REF; i <= (int)MaxArch; ++i) {
            ui8 result[32];
            auto argon2d = factory.Create(
                (NArgonish::EInstructionSet)i, NArgonish::EArgon2Type::Argon2d,
                2, 32, 1, GenKatSecret, sizeof(GenKatSecret));

            argon2d->Hash(GenKatPassword, sizeof(GenKatPassword), GenKatSalt, sizeof(GenKatSalt),
                          result, sizeof(result), GenKatAAD, sizeof(GenKatAAD));

            UNIT_ASSERT(memcmp(result, TResult, sizeof(result)) == 0);

            UNIT_ASSERT(argon2d->Verify(GenKatPassword, sizeof(GenKatPassword),
                                        GenKatSalt, sizeof(GenKatSalt),
                                        TResult, sizeof(TResult),
                                        GenKatAAD, sizeof(GenKatAAD)));
        }
    }

    Y_UNIT_TEST(Argon2i_2p) {
        const ui8 TResult[32] = {
            0xc1, 0x0f, 0x00, 0x5e, 0xf8, 0x78, 0xc8, 0x07,
            0x0e, 0x2c, 0xc5, 0x2f, 0x57, 0x75, 0x25, 0xc9,
            0x71, 0xc7, 0x30, 0xeb, 0x00, 0x64, 0x4a, 0x4e,
            0x26, 0xd0, 0x6e, 0xad, 0x75, 0x46, 0xe0, 0x44};

        NArgonish::TArgon2Factory factory;
        for (int i = (int)NArgonish::EInstructionSet::REF; i <= (int)MaxArch; ++i) {
            ui8 result[32];
            auto argon2i = factory.Create(
                (NArgonish::EInstructionSet)i, NArgonish::EArgon2Type::Argon2i,
                2, 32, 1, GenKatSecret, sizeof(GenKatSecret));

            argon2i->Hash(GenKatPassword, sizeof(GenKatPassword), GenKatSalt, sizeof(GenKatSalt),
                          result, sizeof(result), GenKatAAD, sizeof(GenKatAAD));

            UNIT_ASSERT(memcmp(result, TResult, sizeof(result)) == 0);

            UNIT_ASSERT(argon2i->Verify(GenKatPassword, sizeof(GenKatPassword),
                                        GenKatSalt, sizeof(GenKatSalt),
                                        TResult, sizeof(TResult),
                                        GenKatAAD, sizeof(GenKatAAD)));
        }
    }

    Y_UNIT_TEST(Argon2id_2p) {
        const ui8 TResult[32] = {
            0x6c, 0x00, 0xb7, 0xa9, 0x00, 0xe5, 0x00, 0x4c,
            0x24, 0x46, 0x9e, 0xc1, 0xe7, 0xc0, 0x1a, 0x99,
            0xb2, 0xb8, 0xf7, 0x73, 0x75, 0xd4, 0xec, 0xa7,
            0xd8, 0x08, 0x42, 0x11, 0xd3, 0x23, 0x6b, 0x7a};

        NArgonish::TArgon2Factory factory;
        for (int i = (int)NArgonish::EInstructionSet::REF; i <= (int)MaxArch; ++i) {
            ui8 result[32];
            auto argon2id = factory.Create(
                (NArgonish::EInstructionSet)i, NArgonish::EArgon2Type::Argon2id,
                2, 32, 1, GenKatSecret, sizeof(GenKatSecret));

            argon2id->Hash(GenKatPassword, sizeof(GenKatPassword), GenKatSalt, sizeof(GenKatSalt),
                           result, sizeof(result), GenKatAAD, sizeof(GenKatAAD));

            UNIT_ASSERT(memcmp(result, TResult, sizeof(result)) == 0);

            UNIT_ASSERT(argon2id->Verify(GenKatPassword, sizeof(GenKatPassword),
                                         GenKatSalt, sizeof(GenKatSalt),
                                         TResult, sizeof(TResult),
                                         GenKatAAD, sizeof(GenKatAAD)));
        }
    }

    Y_UNIT_TEST(Argon2d_2p_2th) {
        const ui8 TResult[32] = {
            0x2b, 0x47, 0x35, 0x39, 0x4a, 0x40, 0x3c, 0xc9,
            0x05, 0xfb, 0x51, 0x25, 0x96, 0x68, 0x64, 0x43,
            0x02, 0x16, 0x38, 0xa6, 0xc1, 0x58, 0xfc, 0x8d,
            0xbf, 0x35, 0x73, 0x9a, 0xdb, 0x31, 0x0c, 0x60};

        NArgonish::TArgon2Factory factory;
        for (int i = (int)NArgonish::EInstructionSet::REF; i <= (int)MaxArch; ++i) {
            ui8 result[32];
            auto argon2d = factory.Create(
                (NArgonish::EInstructionSet)i, NArgonish::EArgon2Type::Argon2d,
                2, 32, 2, GenKatSecret, sizeof(GenKatSecret));

            argon2d->Hash(GenKatPassword, sizeof(GenKatPassword), GenKatSalt, sizeof(GenKatSalt),
                          result, sizeof(result), GenKatAAD, sizeof(GenKatAAD));

            UNIT_ASSERT(memcmp(result, TResult, sizeof(result)) == 0);

            UNIT_ASSERT(argon2d->Verify(GenKatPassword, sizeof(GenKatPassword),
                                        GenKatSalt, sizeof(GenKatSalt),
                                        TResult, sizeof(TResult),
                                        GenKatAAD, sizeof(GenKatAAD)));
        }
    }

    Y_UNIT_TEST(Argon2id_2p_4th) {
        const ui8 TResult[32] = {
            0x4f, 0x93, 0xb5, 0xad, 0x78, 0xa4, 0xa9, 0x49,
            0xfb, 0xe3, 0x55, 0x96, 0xd5, 0xa0, 0xc2, 0xab,
            0x6f, 0x52, 0x2d, 0x2d, 0x29, 0xbc, 0x98, 0x49,
            0xca, 0x92, 0xaa, 0xae, 0xba, 0x05, 0x29, 0xd8};

        NArgonish::TArgon2Factory factory;
        for (int i = (int)NArgonish::EInstructionSet::REF; i <= (int)MaxArch; ++i) {
            ui8 result[32];
            auto argon2id = factory.Create(
                (NArgonish::EInstructionSet)i, NArgonish::EArgon2Type::Argon2id,
                2, 64, 4, GenKatSecret, sizeof(GenKatSecret));

            argon2id->Hash(GenKatPassword, sizeof(GenKatPassword), GenKatSalt, sizeof(GenKatSalt),
                           result, sizeof(result), GenKatAAD, sizeof(GenKatAAD));

            UNIT_ASSERT(memcmp(result, TResult, sizeof(result)) == 0);

            UNIT_ASSERT(argon2id->Verify(GenKatPassword, sizeof(GenKatPassword),
                                         GenKatSalt, sizeof(GenKatSalt),
                                         TResult, sizeof(TResult),
                                         GenKatAAD, sizeof(GenKatAAD)));
        }
    }

    Y_UNIT_TEST(Argon2d_2p_4th) {
        const ui8 TResult[32] = {
            0x8f, 0xa2, 0x7c, 0xed, 0x28, 0x38, 0x79, 0x0f,
            0xba, 0x5c, 0x11, 0x85, 0x1c, 0xdf, 0x90, 0x88,
            0xb2, 0x18, 0x44, 0xd7, 0xf0, 0x4c, 0x97, 0xb2,
            0xca, 0xaf, 0xe4, 0xdc, 0x61, 0x4c, 0xae, 0xb2};

        NArgonish::TArgon2Factory factory;
        for (int i = (int)NArgonish::EInstructionSet::REF; i <= (int)MaxArch; ++i) {
            ui8 result[32];
            auto argon2d = factory.Create(
                (NArgonish::EInstructionSet)i, NArgonish::EArgon2Type::Argon2d,
                2, 64, 4, GenKatSecret, sizeof(GenKatSecret));

            argon2d->Hash(GenKatPassword, sizeof(GenKatPassword), GenKatSalt, sizeof(GenKatSalt),
                          result, sizeof(result), GenKatAAD, sizeof(GenKatAAD));

            UNIT_ASSERT(memcmp(result, TResult, sizeof(result)) == 0);

            UNIT_ASSERT(argon2d->Verify(GenKatPassword, sizeof(GenKatPassword),
                                        GenKatSalt, sizeof(GenKatSalt),
                                        TResult, sizeof(TResult),
                                        GenKatAAD, sizeof(GenKatAAD)));
        }
    }

    Y_UNIT_TEST(Argon2i_2p_4th) {
        const ui8 TResult[32] = {
            0x61, 0x1c, 0x99, 0x3c, 0xb0, 0xb7, 0x23, 0x16,
            0xbd, 0xa2, 0x6c, 0x4c, 0x2f, 0xe8, 0x2d, 0x39,
            0x9c, 0x8f, 0x1c, 0xfd, 0x45, 0xd9, 0x58, 0xa9,
            0xb4, 0x9c, 0x6c, 0x64, 0xaf, 0xf0, 0x79, 0x0b};

        NArgonish::TArgon2Factory factory;
        for (int i = (int)NArgonish::EInstructionSet::REF; i <= (int)MaxArch; ++i) {
            ui8 result[32];
            auto argon2i = factory.Create(
                (NArgonish::EInstructionSet)i, NArgonish::EArgon2Type::Argon2i,
                2, 64, 4, GenKatSecret, sizeof(GenKatSecret));

            argon2i->Hash(GenKatPassword, sizeof(GenKatPassword), GenKatSalt, sizeof(GenKatSalt),
                          result, sizeof(result), GenKatAAD, sizeof(GenKatAAD));

            UNIT_ASSERT(memcmp(result, TResult, sizeof(result)) == 0);

            UNIT_ASSERT(argon2i->Verify(GenKatPassword, sizeof(GenKatPassword),
                                        GenKatSalt, sizeof(GenKatSalt),
                                        TResult, sizeof(TResult),
                                        GenKatAAD, sizeof(GenKatAAD)));
        }
    }

    Y_UNIT_TEST(Argon2d_128) {
        const ui8 TResult[128] = {
            0x4e, 0xc4, 0x6c, 0x4e, 0x8c, 0x32, 0x89, 0x65,
            0xf9, 0x82, 0x2b, 0x00, 0x95, 0x00, 0x50, 0x0a,
            0x72, 0x0d, 0xc5, 0x12, 0x8d, 0x6b, 0xbd, 0x84,
            0x7a, 0xf0, 0x78, 0x5d, 0xa6, 0x14, 0xe3, 0xf1,
            0xac, 0x07, 0x1c, 0xca, 0x12, 0x4d, 0x32, 0xa4,
            0x24, 0x08, 0x5e, 0x07, 0x7c, 0x26, 0xb9, 0x1b,
            0x5c, 0xc0, 0xff, 0xb8, 0x7a, 0x20, 0x00, 0xcb,
            0x07, 0x2b, 0xb4, 0x4d, 0x7b, 0x5b, 0x79, 0x9e,
            0xb4, 0x21, 0xcb, 0x63, 0xeb, 0x46, 0xd7, 0x79,
            0x44, 0x9c, 0x9f, 0xee, 0xa4, 0x17, 0xb5, 0x01,
            0x0f, 0x61, 0x7e, 0xd8, 0xec, 0x1b, 0xe3, 0x8b,
            0x9a, 0x74, 0x17, 0x19, 0x9d, 0x80, 0xe9, 0x20,
            0xd4, 0x84, 0xdd, 0x07, 0x40, 0xb2, 0x26, 0xdb,
            0xf7, 0xbe, 0x79, 0x7f, 0x81, 0x59, 0x86, 0xf3,
            0xe9, 0x34, 0xe4, 0x52, 0xcd, 0x33, 0xb9, 0xf8,
            0x9e, 0x62, 0x65, 0x89, 0xbb, 0xce, 0x7d, 0x65};

        NArgonish::TArgon2Factory factory;
        for (int i = (int)NArgonish::EInstructionSet::REF; i <= (int)MaxArch; ++i) {
            ui8 result[128];
            auto argon2d = factory.Create(
                (NArgonish::EInstructionSet)i, NArgonish::EArgon2Type::Argon2d,
                1, 32, 1, GenKatSecret, sizeof(GenKatSecret));

            argon2d->Hash(GenKatPassword, sizeof(GenKatPassword), GenKatSalt, sizeof(GenKatSalt),
                          result, sizeof(result), GenKatAAD, sizeof(GenKatAAD));

            UNIT_ASSERT(memcmp(result, TResult, sizeof(result)) == 0);

            UNIT_ASSERT(argon2d->Verify(GenKatPassword, sizeof(GenKatPassword),
                                        GenKatSalt, sizeof(GenKatSalt),
                                        TResult, sizeof(TResult),
                                        GenKatAAD, sizeof(GenKatAAD)));
        }
    }

    Y_UNIT_TEST(Blake2B_16_ABC) {
        const ui8 TResult[16] = {
            0xcf, 0x4a, 0xb7, 0x91, 0xc6, 0x2b, 0x8d, 0x2b,
            0x21, 0x09, 0xc9, 0x02, 0x75, 0x28, 0x78, 0x16};
        const ui8 data[] = {'a', 'b', 'c'};

        NArgonish::TBlake2BFactory factory;
        for (int i = (int)NArgonish::EInstructionSet::REF; i <= (int)MaxArch; ++i) {
            auto blake2b = factory.Create((NArgonish::EInstructionSet)i, sizeof(TResult));
            ui8 hashResult[16] = {0};

            blake2b->Update(data, sizeof(data));
            blake2b->Final(hashResult, sizeof(hashResult));

            UNIT_ASSERT(memcmp(hashResult, TResult, sizeof(TResult)) == 0);
        }
    }

    Y_UNIT_TEST(Blake2B_64_ABC) {
        const ui8 TResult[64] = {
            0xba, 0x80, 0xa5, 0x3f, 0x98, 0x1c, 0x4d, 0x0d,
            0x6a, 0x27, 0x97, 0xb6, 0x9f, 0x12, 0xf6, 0xe9,
            0x4c, 0x21, 0x2f, 0x14, 0x68, 0x5a, 0xc4, 0xb7,
            0x4b, 0x12, 0xbb, 0x6f, 0xdb, 0xff, 0xa2, 0xd1,
            0x7d, 0x87, 0xc5, 0x39, 0x2a, 0xab, 0x79, 0x2d,
            0xc2, 0x52, 0xd5, 0xde, 0x45, 0x33, 0xcc, 0x95,
            0x18, 0xd3, 0x8a, 0xa8, 0xdb, 0xf1, 0x92, 0x5a,
            0xb9, 0x23, 0x86, 0xed, 0xd4, 0x00, 0x99, 0x23};
        const ui8 data[] = {'a', 'b', 'c'};

        NArgonish::TBlake2BFactory factory;
        for (int i = (int)NArgonish::EInstructionSet::REF; i <= (int)(int)MaxArch; ++i) {
            auto blake2b = factory.Create((NArgonish::EInstructionSet)i, sizeof(TResult));
            ui8 hashResult[64] = {0};

            blake2b->Update(data, sizeof(data));
            blake2b->Final(hashResult, sizeof(hashResult));

            UNIT_ASSERT(memcmp(hashResult, TResult, sizeof(TResult)) == 0);
        }
    }
}
