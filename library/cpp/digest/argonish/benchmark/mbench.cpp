#include <library/cpp/testing/benchmark/bench.h>
#include <library/cpp/digest/argonish/argon2.h>

Y_CPU_BENCHMARK(Argon2d_2048_REF, iface) {
    NArgonish::TArgon2Factory factory;
    auto argon2 = factory.Create(NArgonish::EInstructionSet::REF, NArgonish::EArgon2Type::Argon2d, 1, 2048, 1);
    ui8 password[16] = {0x0};
    ui8 salt[16] = {0x01};
    ui8 result[16] = {0};

    for (ui64 i = 0; i < iface.Iterations(); ++i) {
        argon2->Hash(password, sizeof(password), salt, sizeof(salt), result, sizeof(result));
    }
}

#if !defined(_arm64_)
Y_CPU_BENCHMARK(Argon2d_2048_SSE2, iface) {
    NArgonish::TArgon2Factory factory;
    auto argon2 = factory.Create(NArgonish::EInstructionSet::SSE2, NArgonish::EArgon2Type::Argon2d, 1, 2048, 1);
    ui8 password[16] = {0x0};
    ui8 salt[16] = {0x01};
    ui8 result[16] = {0};

    for (ui64 i = 0; i < iface.Iterations(); ++i) {
        argon2->Hash(password, sizeof(password), salt, sizeof(salt), result, sizeof(result));
    }
}

Y_CPU_BENCHMARK(Argon2d_2048_SSSE3, iface) {
    NArgonish::TArgon2Factory factory;
    auto argon2 = factory.Create(NArgonish::EInstructionSet::SSSE3, NArgonish::EArgon2Type::Argon2d, 1, 2048, 1);
    ui8 password[16] = {0x0};
    ui8 salt[16] = {0x01};
    ui8 result[16] = {0};

    for (ui64 i = 0; i < iface.Iterations(); ++i) {
        argon2->Hash(password, sizeof(password), salt, sizeof(salt), result, sizeof(result));
    }
}

Y_CPU_BENCHMARK(Argon2d_2048_SSE41, iface) {
    NArgonish::TArgon2Factory factory;
    auto argon2 = factory.Create(NArgonish::EInstructionSet::SSE41, NArgonish::EArgon2Type::Argon2d, 1, 2048, 1);
    ui8 password[16] = {0x0};
    ui8 salt[16] = {0x01};
    ui8 result[16] = {0};

    for (ui64 i = 0; i < iface.Iterations(); ++i) {
        argon2->Hash(password, sizeof(password), salt, sizeof(salt), result, sizeof(result));
    }
}

Y_CPU_BENCHMARK(Argon2d_2048_AVX2, iface) {
    NArgonish::TArgon2Factory factory;
    auto argon2 = factory.Create(NArgonish::EInstructionSet::AVX2, NArgonish::EArgon2Type::Argon2d, 1, 2048, 1);
    ui8 password[16] = {0x0};
    ui8 salt[16] = {0x01};
    ui8 result[16] = {0};

    for (ui64 i = 0; i < iface.Iterations(); ++i) {
        argon2->Hash(password, sizeof(password), salt, sizeof(salt), result, sizeof(result));
    }
}
#endif
