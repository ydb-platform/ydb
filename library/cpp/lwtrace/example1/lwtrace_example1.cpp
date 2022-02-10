#include <library/cpp/lwtrace/all.h>

#define LWTRACE_EXAMPLE_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES)          \
    PROBE(IterationProbe, GROUPS(), TYPES(i32, double), NAMES("n", "result")) \
    /**/

LWTRACE_DECLARE_PROVIDER(LWTRACE_EXAMPLE_PROVIDER)
LWTRACE_DEFINE_PROVIDER(LWTRACE_EXAMPLE_PROVIDER)

void InitLWTrace() {
    NLWTrace::StartLwtraceFromEnv();
}

long double Fact(int n) {
    if (n < 0) {
        ythrow yexception() << "N! is undefined for negative N (" << n << ")";
    }
    double result = 1;
    for (; n > 1; --n) {
        GLOBAL_LWPROBE(LWTRACE_EXAMPLE_PROVIDER, IterationProbe, n, result);
        result *= n;
    }
    return result;
}

void FactorialCalculator() {
    i32 n;
    Cout << "Enter a number: ";
    TString str;
    Cin >> n;
    double factN = Fact(n);
    Cout << n << "! = " << factN << Endl << Endl;
}

int main() {
    InitLWTrace();
    FactorialCalculator();
    return 0;
}
