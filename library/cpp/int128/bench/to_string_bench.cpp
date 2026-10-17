#include <library/cpp/testing/benchmark/bench.h>

#include "to_string_legacy.h"

#include <util/generic/xrange.h>
#include <util/system/compiler.h>

// Legacy processes all 128 bits one by one and keeps 39 individual decimal digits.
// Base1e9 consumes four 32-bit binary words and accumulates five base-10^9 words.
// Base1e19 is the production implementation: it divides a {high, low} ui64 pair
// twice and produces three base-10^19 words without 128-bit arithmetic.
// DivisionBy10 repeatedly obtains one decimal digit with number % 10 and then
// removes that digit with number = number / 10 until the 128-bit value becomes zero.
// Each algorithm is measured on the same unsigned and signed input sets.

Y_CPU_BENCHMARK(LegacyToStringUnsigned128, iface) {
    const auto& values = NInt128ToStringBenchmark::GetUnsignedToStringTestValues();
    for (const auto i : xrange(iface.Iterations())) {
        const TString result = NInt128ToStringBenchmark::LegacyToString(values[i % values.size()]);
        Y_FAKE_READ(result);
    }
}

Y_CPU_BENCHMARK(Base1e9ToStringUnsigned128, iface) {
    const auto& values = NInt128ToStringBenchmark::GetUnsignedToStringTestValues();
    for (const auto i : xrange(iface.Iterations())) {
        const TString result = NInt128ToStringBenchmark::Base1e9ToString(values[i % values.size()]);
        Y_FAKE_READ(result);
    }
}

Y_CPU_BENCHMARK(Base1e19ToStringUnsigned128, iface) {
    const auto& values = NInt128ToStringBenchmark::GetUnsignedToStringTestValues();
    for (const auto i : xrange(iface.Iterations())) {
        const TString result = NInt128ToStringBenchmark::Base1e19ToString(values[i % values.size()]);
        Y_FAKE_READ(result);
    }
}

Y_CPU_BENCHMARK(Base1e19ToStringUnsigned128FastPath, iface) {
    const auto& values = NInt128ToStringBenchmark::GetUnsignedToStringFastPathValues();
    for (const auto i : xrange(iface.Iterations())) {
        const TString result = NInt128ToStringBenchmark::Base1e19ToString(values[i % values.size()]);
        Y_FAKE_READ(result);
    }
}

Y_CPU_BENCHMARK(DivisionBy10ToStringUnsigned128, iface) {
    const auto& values = NInt128ToStringBenchmark::GetUnsignedToStringTestValues();
    for (const auto i : xrange(iface.Iterations())) {
        const TString result = NInt128ToStringBenchmark::DivisionBy10ToString(values[i % values.size()]);
        Y_FAKE_READ(result);
    }
}

Y_CPU_BENCHMARK(LegacyToStringSigned128, iface) {
    const auto& values = NInt128ToStringBenchmark::GetSignedToStringTestValues();
    for (const auto i : xrange(iface.Iterations())) {
        const TString result = NInt128ToStringBenchmark::LegacyToString(values[i % values.size()]);
        Y_FAKE_READ(result);
    }
}

Y_CPU_BENCHMARK(Base1e9ToStringSigned128, iface) {
    const auto& values = NInt128ToStringBenchmark::GetSignedToStringTestValues();
    for (const auto i : xrange(iface.Iterations())) {
        const TString result = NInt128ToStringBenchmark::Base1e9ToString(values[i % values.size()]);
        Y_FAKE_READ(result);
    }
}

Y_CPU_BENCHMARK(Base1e19ToStringSigned128, iface) {
    const auto& values = NInt128ToStringBenchmark::GetSignedToStringTestValues();
    for (const auto i : xrange(iface.Iterations())) {
        const TString result = NInt128ToStringBenchmark::Base1e19ToString(values[i % values.size()]);
        Y_FAKE_READ(result);
    }
}

Y_CPU_BENCHMARK(Base1e19ToStringSigned128FastPath, iface) {
    const auto& values = NInt128ToStringBenchmark::GetSignedToStringFastPathValues();
    for (const auto i : xrange(iface.Iterations())) {
        const TString result = NInt128ToStringBenchmark::Base1e19ToString(values[i % values.size()]);
        Y_FAKE_READ(result);
    }
}

Y_CPU_BENCHMARK(DivisionBy10ToStringSigned128, iface) {
    const auto& values = NInt128ToStringBenchmark::GetSignedToStringTestValues();
    for (const auto i : xrange(iface.Iterations())) {
        const TString result = NInt128ToStringBenchmark::DivisionBy10ToString(values[i % values.size()]);
        Y_FAKE_READ(result);
    }
}
