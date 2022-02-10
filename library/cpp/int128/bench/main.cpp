#include <library/cpp/int128/int128.h>
#include <library/cpp/testing/benchmark/bench.h>

#include <util/generic/xrange.h>
#include <util/system/compiler.h>

Y_CPU_BENCHMARK(LibraryDivisionUnsigned128, iface) {
    ui128 b = {0, 10'000'000};
    for (const auto i : xrange(iface.Iterations())) {
        ui128 a = i * 10'000'000;
        ui128 c = a / b;
        Y_FAKE_READ(c);
    }
}

#if defined(Y_HAVE_INT128)
Y_CPU_BENCHMARK(IntrinsicDivisionUnsigned128, iface) {
    unsigned __int128 b = 10'000'000;
    for (const auto i : xrange(iface.Iterations())) {
        unsigned __int128 a = i * 10'000'000;
        unsigned __int128 c = a / b;
        Y_FAKE_READ(c);
    }
}
#endif // Y_HAVE_INT128

Y_CPU_BENCHMARK(LibraryDivisionSigned128, iface) {
    i128 b = {0, 10'000'000};
    for (const auto i : xrange(iface.Iterations())) {
        i128 a = i * 10'000'000;
        i128 c = a / b;
        Y_FAKE_READ(c);
    }
}

#if defined(Y_HAVE_INT128)
Y_CPU_BENCHMARK(IntrinsicDivisionSigned128, iface) {
    signed __int128 b = 10'000'000;
    for (const auto i : xrange(iface.Iterations())) {
        signed __int128 a = i * 10'000'000;
        signed __int128 c = a / b;
        Y_FAKE_READ(c);
    }
}
#endif // Y_HAVE_INT128
