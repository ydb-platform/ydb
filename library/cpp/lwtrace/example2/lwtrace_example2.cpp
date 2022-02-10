#include <library/cpp/lwtrace/control.h>
#include <library/cpp/lwtrace/all.h>

#include <library/cpp/getopt/last_getopt.h>
#include <google/protobuf/text_format.h>
#include <util/stream/file.h>

#define LWTRACE_EXAMPLE_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES)                           \
    PROBE(StartupProbe, GROUPS(), TYPES(), NAMES())                                            \
    PROBE(IterationProbe, GROUPS(), TYPES(i64, double), NAMES("n", "result"))                  \
    PROBE(DurationProbe, GROUPS(), TYPES(ui64, i64, double), NAMES("duration", "n", "result")) \
    PROBE(ResultProbe, GROUPS(), TYPES(double), NAMES("factN"))                                \
    PROBE(AfterInputProbe, GROUPS(), TYPES(i32), NAMES("n"))                                   \
    /**/

LWTRACE_DECLARE_PROVIDER(LWTRACE_EXAMPLE_PROVIDER)
LWTRACE_DEFINE_PROVIDER(LWTRACE_EXAMPLE_PROVIDER)

THolder<NLWTrace::TManager> traceManager;

struct TConfig {
    bool UnsafeLWTrace;
    TString TraceRequestPath;
};

void InitLWTrace(TConfig& cfg) {
    traceManager.Reset(new NLWTrace::TManager(*Singleton<NLWTrace::TProbeRegistry>(), cfg.UnsafeLWTrace));
}

void AddLWTraceRequest(TConfig& cfg) {
    TString queryStr = TUnbufferedFileInput(cfg.TraceRequestPath).ReadAll();
    NLWTrace::TQuery query;
    google::protobuf::TextFormat::ParseFromString(queryStr, &query);
    traceManager->New("TraceRequest1", query);
}

class TLogReader {
public:
    void Push(TThread::TId tid, const NLWTrace::TCyclicLog::TItem& item) {
        Cout << "tid=" << tid << " probe=" << item.Probe->Event.Name;
        if (item.Timestamp != TInstant::Zero()) {
            Cout << " time=" << item.Timestamp;
        }
        if (item.SavedParamsCount > 0) {
            TString paramValues[LWTRACE_MAX_PARAMS];
            item.Probe->Event.Signature.SerializeParams(item.Params, paramValues);
            Cout << " params: ";
            for (size_t i = 0; i < item.SavedParamsCount; ++i) {
                Cout << " " << item.Probe->Event.Signature.ParamNames[i] << "=" << paramValues[i];
            }
        }
        Cout << Endl;
    }
};

void DisplayLWTraceLog() {
    Cout << "LWTrace log:" << Endl;
    TLogReader reader;
    traceManager->ReadLog("TraceRequest1", reader);
}

long double Fact(i64 n) {
    if (n < 0) {
        ythrow yexception() << "N! is undefined for negative N (" << n << ")";
    }
    double result = 1;
    for (; n > 1; --n) {
        GLOBAL_LWPROBE(LWTRACE_EXAMPLE_PROVIDER, IterationProbe, n, result);
        GLOBAL_LWPROBE_DURATION(LWTRACE_EXAMPLE_PROVIDER, DurationProbe, n, result);

        result *= n;
    }
    return result;
}

void FactorialCalculator() {
    GLOBAL_LWPROBE(LWTRACE_EXAMPLE_PROVIDER, StartupProbe);

    i32 n;
    Cout << "Enter a number: ";
    TString str;
    Cin >> n;

    GLOBAL_LWPROBE(LWTRACE_EXAMPLE_PROVIDER, AfterInputProbe, n);

    double factN = Fact(n);
    Cout << n << "! = " << factN << Endl << Endl;

    GLOBAL_LWPROBE(LWTRACE_EXAMPLE_PROVIDER, ResultProbe, factN);
}

int main(int argc, char** argv) {
    TConfig cfg;
    using namespace NLastGetopt;
    TOpts opts = NLastGetopt::TOpts::Default();
    opts.AddLongOption('u', "unsafe-lwtrace",
                       "allow destructive LWTrace actions")
        .OptionalValue(ToString(true))
        .DefaultValue(ToString(false))
        .StoreResult(&cfg.UnsafeLWTrace);
    opts.AddLongOption('f', "trace-request",
                       "specify a file containing LWTrace request")
        .DefaultValue("example_query.tr")
        .StoreResult(&cfg.TraceRequestPath);
    opts.AddHelpOption('h');
    TOptsParseResult res(&opts, argc, argv);

    InitLWTrace(cfg);

    AddLWTraceRequest(cfg);

    FactorialCalculator();

    DisplayLWTraceLog();

    return 0;
}
