#include <library/cpp/lwtrace/all.h>
#include <library/cpp/lwtrace/lwprobe.h>

template <ui64 N>
ui64 Fact() {
    ui64 result = N * Fact<N - 1>();

#ifndef LWTRACE_DISABLE
    // Note that probe is create on the first pass
    // LWTRACE_DECLARE_PROVIDER and LWTRACE_DEFINE_PROVIDER are not needed
    // (Provider is created implicitly)
    static NLWTrace::TLWProbe<ui64> factProbe(
        "LWTRACE_EXAMPLE_PROVIDER", "FactProbe_" + ToString(N), {}, {"result"});

    LWPROBE_OBJ(factProbe, result);
#endif // LWTRACE_DISABLE
    return result;
}

template <>
ui64 Fact<0>() {
    return 1;
}

int main() {
    Fact<6>();                       // First run is required to create probes we can use later in trace query
    NLWTrace::StartLwtraceFromEnv(); // parse trace query and create trace session
    Cout << Fact<6>() << Endl;       // actually trigger probes
    return 0;
}
