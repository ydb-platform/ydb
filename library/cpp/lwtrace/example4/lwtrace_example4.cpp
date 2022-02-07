#include <library/cpp/lwtrace/all.h>

#define LWTRACE_EXAMPLE_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES)     \
    PROBE(BackTrack, GROUPS(), TYPES(NLWTrace::TSymbol), NAMES("frame")) \
    /**/

LWTRACE_DECLARE_PROVIDER(LWTRACE_EXAMPLE_PROVIDER)
LWTRACE_DEFINE_PROVIDER(LWTRACE_EXAMPLE_PROVIDER)

LWTRACE_USING(LWTRACE_EXAMPLE_PROVIDER);

#define MY_BACKTRACK() LWPROBE(BackTrack, LWTRACE_LOCATION_SYMBOL)

void InitLWTrace() {
    NLWTrace::StartLwtraceFromEnv();
}

long double Fact(int n) {
    MY_BACKTRACK();
    if (n < 0) {
        MY_BACKTRACK();
        ythrow yexception() << "N! is undefined for negative N (" << n << ")";
    }
    double result = 1;
    for (; n > 1; --n) {
        MY_BACKTRACK();
        result *= n;
    }
    MY_BACKTRACK();
    return result;
}

void FactorialCalculator() {
    MY_BACKTRACK();
    i32 n;
    Cout << "Enter a number: ";
    TString str;
    Cin >> n;
    double factN = Fact(n);
    Cout << n << "! = " << factN << Endl << Endl;
}

int main() {
    InitLWTrace();
    MY_BACKTRACK();
    FactorialCalculator();
    MY_BACKTRACK();
    return 0;
}
