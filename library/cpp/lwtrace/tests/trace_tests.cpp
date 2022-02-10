#include <library/cpp/lwtrace/all.h>

#include <library/cpp/getopt/last_getopt.h>

#include <google/protobuf/text_format.h>

#include <util/system/pipe.h>
#include <util/generic/ymath.h>
#include <util/string/printf.h>
#include <util/string/vector.h>

#define LWTRACE_TESTS_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES)                                           \
    PROBE(Simplest, GROUPS("Group"), TYPES(), NAMES())                                                       \
    PROBE(IntParam, GROUPS("Group"), TYPES(ui32), NAMES("value"))                                            \
    PROBE(StringParam, GROUPS("Group"), TYPES(TString), NAMES("value"))                                      \
    PROBE(SymbolParam, GROUPS("Group"), TYPES(NLWTrace::TSymbol), NAMES("symbol"))                           \
    PROBE(CheckParam, GROUPS("Group"), TYPES(NLWTrace::TCheck), NAMES("value"))                              \
    EVENT(TwoParamsEvent, GROUPS("Group"), TYPES(int, TString), NAMES("param1", "param2"))                   \
    EVENT(TwoParamsCheckEvent, GROUPS("Group"), TYPES(NLWTrace::TCheck, TString), NAMES("param1", "param2")) \
    /**/

LWTRACE_DECLARE_PROVIDER(LWTRACE_TESTS_PROVIDER)
LWTRACE_DEFINE_PROVIDER(LWTRACE_TESTS_PROVIDER)
LWTRACE_USING(LWTRACE_TESTS_PROVIDER)

namespace NLWTrace {
    namespace NTests {
        TString gStrValue = "a long string value that can be possible passed as a trace probe string parameter";
        //TString gStrValue = "short";

        LWTRACE_DEFINE_SYMBOL(gSymbol, "a long symbol value that can be possible passed as a trace probe string parameter");

        struct TConfig {
            size_t Cycles;
            size_t Runs;
            bool UnsafeLWTrace;

            TConfig() {
                Cycles = 100000;
                Runs = 10;
                UnsafeLWTrace = false;
            }
        };

        struct TMeasure {
            double Average;
            double Sigma;
            TMeasure(double a, double s)
                : Average(a)
                , Sigma(s)
            {
            }
        };

#define DEFINE_MEASUREMENT(name, ...)                                             \
    double name##OneRun(const TConfig& cfg) {                                     \
        TInstant t0 = Now();                                                      \
        for (size_t i = 0; i < cfg.Cycles; i++) {                                 \
            HOTSPOT(name, ##__VA_ARGS__);                                         \
            HOTSPOT(name, ##__VA_ARGS__);                                         \
            HOTSPOT(name, ##__VA_ARGS__);                                         \
            HOTSPOT(name, ##__VA_ARGS__);                                         \
            HOTSPOT(name, ##__VA_ARGS__);                                         \
            HOTSPOT(name, ##__VA_ARGS__);                                         \
            HOTSPOT(name, ##__VA_ARGS__);                                         \
            HOTSPOT(name, ##__VA_ARGS__);                                         \
            HOTSPOT(name, ##__VA_ARGS__);                                         \
            HOTSPOT(name, ##__VA_ARGS__);                                         \
        }                                                                         \
        TInstant t1 = Now();                                                      \
        return double(t1.MicroSeconds() - t0.MicroSeconds()) / (cfg.Cycles * 10); \
    }                                                                             \
    TMeasure name##Time(const TConfig& cfg) {                                     \
        double v = 0;                                                             \
        double vSq = 0;                                                           \
        for (size_t i = 0; i < cfg.Runs; i++) {                                   \
            double value = name##OneRun(cfg);                                     \
            v += value;                                                           \
            vSq += value * value;                                                 \
        }                                                                         \
        v /= cfg.Runs;                                                            \
        vSq /= cfg.Runs;                                                          \
        return TMeasure(v, sqrt(vSq - v * v));                                    \
    }                                                                             \
    /**/

        class TProbes: public TProbeRegistry {
        public:
            TManager Mngr;

            TProbes(bool destructiveActionsAllowed)
                : Mngr(*this, destructiveActionsAllowed)
            {
                AddProbesList(LWTRACE_GET_PROBES(LWTRACE_TESTS_PROVIDER));
            }

#define HOTSPOT(name, ...) LWPROBE(name, ##__VA_ARGS__);
            DEFINE_MEASUREMENT(Simplest);
            DEFINE_MEASUREMENT(IntParam, 123);
            DEFINE_MEASUREMENT(StringParam, gStrValue);
            DEFINE_MEASUREMENT(SymbolParam, gSymbol);
            DEFINE_MEASUREMENT(CheckParam, TCheck(13));
#undef HOTSPOT
        };

        class TInMemoryLogTest {
        public:
            TInMemoryLog Log;

            TInMemoryLogTest()
                : Log(1000)
            {
            }

#define HOTSPOT(name, ...) LWEVENT(name, Log, true, ##__VA_ARGS__);
            DEFINE_MEASUREMENT(TwoParamsEvent, 666, TString("bla-bla-bla"));
#undef HOTSPOT
        };

        class TInMemoryLogCheckTest {
        public:
            TInMemoryLog Log;

            TInMemoryLogCheckTest()
                : Log(1000)
            {
            }

#define HOTSPOT(name, ...) LWEVENT(name, Log, true, ##__VA_ARGS__);
            DEFINE_MEASUREMENT(TwoParamsCheckEvent, TCheck(666), TString("bla-bla-bla"));
#undef HOTSPOT
        };

        NLWTrace::TQuery MakeQuery(const TString& queryStr) {
            NLWTrace::TQuery query;
            google::protobuf::TextFormat::ParseFromString(queryStr, &query);
            return query;
        }

        void NoExec(const TConfig& cfg) {
            TProbes p(cfg.UnsafeLWTrace);
            Cout << "call to probe w/o executor: " << p.SimplestTime(cfg) << Endl;
        }

        void Log(const TConfig& cfg) {
            TProbes p(cfg.UnsafeLWTrace);
            p.Mngr.New("test-trace", MakeQuery(
                                         "Blocks {"
                                         "    ProbeDesc {"
                                         "        Name: \"Simplest\""
                                         "        Provider: \"LWTRACE_TESTS_PROVIDER\""
                                         "    }"
                                         "    Action {"
                                         "        LogAction {"
                                         "            LogTimestamp: false"
                                         "        }"
                                         "    }"
                                         "}"));
            Cout << "call to probe with logging executor: " << p.SimplestTime(cfg) << Endl;
        }

        void LogTs(const TConfig& cfg) {
            TProbes p(cfg.UnsafeLWTrace);
            p.Mngr.New("test-trace", MakeQuery(
                                         "Blocks {"
                                         "    ProbeDesc {"
                                         "        Name: \"Simplest\""
                                         "        Provider: \"LWTRACE_TESTS_PROVIDER\""
                                         "    }"
                                         "    Action {"
                                         "        LogAction {"
                                         "            LogTimestamp: true"
                                         "        }"
                                         "    }"
                                         "}"));
            Cout << "call to probe with logging (+timestamp) executor: " << p.SimplestTime(cfg) << Endl;
        }

        void FalseIntFilter(const TConfig& cfg) {
            TProbes p(cfg.UnsafeLWTrace);
            p.Mngr.New("test-trace", MakeQuery(
                                         "Blocks {"
                                         "    ProbeDesc {"
                                         "        Name: \"IntParam\""
                                         "        Provider: \"LWTRACE_TESTS_PROVIDER\""
                                         "    }"
                                         "    Predicate {"
                                         "        Operators {"
                                         "            Type: OT_GT"
                                         "            Argument {"
                                         "                Param: \"value\""
                                         "            }"
                                         "            Argument {"
                                         "                Value: \"1000\""
                                         "            }"
                                         "        }"
                                         "    }"
                                         "    Action {"
                                         "        LogAction {"
                                         "            LogTimestamp: false"
                                         "        }"
                                         "    }"
                                         "}"));
            Cout << "call to probe with int filter (always false) executor: " << p.IntParamTime(cfg) << Endl;
        }

        void LogIntAfterFilter(const TConfig& cfg) {
            TProbes p(cfg.UnsafeLWTrace);
            p.Mngr.New("test-trace", MakeQuery(
                                         "Blocks {"
                                         "    ProbeDesc {"
                                         "        Name: \"IntParam\""
                                         "        Provider: \"LWTRACE_TESTS_PROVIDER\""
                                         "    }"
                                         "    Predicate {"
                                         "        Operators {"
                                         "            Type: OT_GT"
                                         "            Argument {"
                                         "                Param: \"value\""
                                         "            }"
                                         "            Argument {"
                                         "                Value: \"0\""
                                         "            }"
                                         "        }"
                                         "    }"
                                         "    Action {"
                                         "        LogAction {"
                                         "            LogTimestamp: false"
                                         "        }"
                                         "    }"
                                         "}"));
            Cout << "call to probe with int filter (always true) and log executors: " << p.IntParamTime(cfg) << Endl;
        }

        void LogInt(const TConfig& cfg) {
            TProbes p(cfg.UnsafeLWTrace);
            p.Mngr.New("test-trace", MakeQuery(
                                         "Blocks {"
                                         "    ProbeDesc {"
                                         "        Name: \"IntParam\""
                                         "        Provider: \"LWTRACE_TESTS_PROVIDER\""
                                         "    }"
                                         "    Action {"
                                         "        LogAction {"
                                         "            LogTimestamp: false"
                                         "        }"
                                         "    }"
                                         "}"));
            Cout << "call to int probe with log executor: " << p.IntParamTime(cfg) << Endl;
        }

        void FalseStringFilter(const TConfig& cfg) {
            TProbes p(cfg.UnsafeLWTrace);
            p.Mngr.New("test-trace", MakeQuery(
                                         "Blocks {"
                                         "    ProbeDesc {"
                                         "        Name: \"StringParam\""
                                         "        Provider: \"LWTRACE_TESTS_PROVIDER\""
                                         "    }"
                                         "    Predicate {"
                                         "        Operators {"
                                         "            Type: OT_EQ"
                                         "            Argument {"
                                         "                Param: \"value\""
                                         "            }"
                                         "            Argument {"
                                         "                Value: \"string that never can exist\""
                                         "            }"
                                         "        }"
                                         "    }"
                                         "    Action {"
                                         "        LogAction {"
                                         "            LogTimestamp: false"
                                         "        }"
                                         "    }"
                                         "}"));
            Cout << "call to probe with string filter (always false) executor: " << p.StringParamTime(cfg) << Endl;
        }

        void FalseStringFilterPartialMatch(const TConfig& cfg) {
            TProbes p(cfg.UnsafeLWTrace);
            p.Mngr.New("test-trace", MakeQuery(
                                         "Blocks {"
                                         "    ProbeDesc {"
                                         "        Name: \"StringParam\""
                                         "        Provider: \"LWTRACE_TESTS_PROVIDER\""
                                         "    }"
                                         "    Predicate {"
                                         "        Operators {"
                                         "            Type: OT_EQ"
                                         "            Argument {"
                                         "                Param: \"value\""
                                         "            }"
                                         "            Argument {"
                                         "                Value: \"" +
                                         gStrValue + "-not-full-match\""
                                                     "            }"
                                                     "        }"
                                                     "    }"
                                                     "    Action {"
                                                     "        LogAction {"
                                                     "            LogTimestamp: false"
                                                     "        }"
                                                     "    }"
                                                     "}"));
            Cout << "call to probe with string filter (always false) executor: " << p.StringParamTime(cfg) << Endl;
        }

        void LogStringAfterFilter(const TConfig& cfg) {
            TProbes p(cfg.UnsafeLWTrace);
            p.Mngr.New("test-trace", MakeQuery(
                                         "Blocks {"
                                         "    ProbeDesc {"
                                         "        Name: \"StringParam\""
                                         "        Provider: \"LWTRACE_TESTS_PROVIDER\""
                                         "    }"
                                         "    Predicate {"
                                         "        Operators {"
                                         "            Type: OT_EQ"
                                         "            Argument {"
                                         "                Param: \"value\""
                                         "            }"
                                         "            Argument {"
                                         "                Value: \"" +
                                         gStrValue + "\""
                                                     "            }"
                                                     "        }"
                                                     "    }"
                                                     "    Action {"
                                                     "        LogAction {"
                                                     "            LogTimestamp: false"
                                                     "        }"
                                                     "    }"
                                                     "}"));
            Cout << "call to probe with string filter (always true) and log executors: " << p.StringParamTime(cfg) << Endl;
        }

        void LogString(const TConfig& cfg) {
            TProbes p(cfg.UnsafeLWTrace);
            p.Mngr.New("test-trace", MakeQuery(
                                         "Blocks {"
                                         "    ProbeDesc {"
                                         "        Name: \"StringParam\""
                                         "        Provider: \"LWTRACE_TESTS_PROVIDER\""
                                         "    }"
                                         "    Action {"
                                         "        LogAction {"
                                         "            LogTimestamp: false"
                                         "        }"
                                         "    }"
                                         "}"));
            Cout << "call to string probe with log executor: " << p.StringParamTime(cfg) << Endl;
        }

        void LogSymbol(const TConfig& cfg) {
            TProbes p(cfg.UnsafeLWTrace);
            p.Mngr.New("test-trace", MakeQuery(
                                         "Blocks {"
                                         "    ProbeDesc {"
                                         "        Name: \"SymbolParam\""
                                         "        Provider: \"LWTRACE_TESTS_PROVIDER\""
                                         "    }"
                                         "    Action {"
                                         "        LogAction {"
                                         "            LogTimestamp: false"
                                         "        }"
                                         "    }"
                                         "}"));
            Cout << "call to symbol probe with log executor: " << p.SymbolParamTime(cfg) << Endl;
        }

        void LogCheck(const TConfig& cfg) {
            TProbes p(cfg.UnsafeLWTrace);
            p.Mngr.New("test-trace", MakeQuery(
                                         "Blocks {"
                                         "    ProbeDesc {"
                                         "        Name: \"CheckParam\""
                                         "        Provider: \"LWTRACE_TESTS_PROVIDER\""
                                         "    }"
                                         "    Action {"
                                         "        LogAction {"
                                         "            LogTimestamp: false"
                                         "        }"
                                         "    }"
                                         "}"));
            Cout << "call to check probe with log executor: " << p.CheckParamTime(cfg) << Endl;
        }

        void InMemoryLog(const TConfig& cfg) {
            TInMemoryLogTest test;
            Cout << "log to in-memory log with (int, string) writer: " << test.TwoParamsEventTime(cfg) << Endl;
        }

        void InMemoryLogCheck(const TConfig& cfg) {
            TInMemoryLogCheckTest test;
            Cout << "log to in-memory log with (leak-check, string) writer: " << test.TwoParamsCheckEventTime(cfg) << Endl;
        }

        void MultipleActionsCheck(const TConfig& cfg) {
            TProbes p(cfg.UnsafeLWTrace);
            p.Mngr.New("test-multipleActions", MakeQuery(
                                                   "Blocks {"
                                                   "   ProbeDesc {"
                                                   "       Name: \"Simplest\""
                                                   "       Provider: \"LWTRACE_TESTS_PROVIDER\""
                                                   "   }"
                                                   "   Predicate {"
                                                   "       Operators {"
                                                   "           Type: OT_LT"
                                                   "           Argument {"
                                                   "               Variable: \"counter\""
                                                   "           }"
                                                   "           Argument {"
                                                   "               Value: \"2\""
                                                   "           }"
                                                   "       }"
                                                   "   }"
                                                   "   Action {"
                                                   "       StatementAction {"
                                                   "           Type: ST_INC"
                                                   "           Argument {"
                                                   "               Variable: \"counter\""
                                                   "           }"
                                                   "       }"
                                                   "   }"
                                                   "   Action {"
                                                   "       StatementAction {"
                                                   "           Type: ST_DEC"
                                                   "           Argument {"
                                                   "               Variable: \"counter\""
                                                   "           }"
                                                   "       }"
                                                   "   }"
                                                   "   Action {"
                                                   "       StatementAction {"
                                                   "           Type: ST_SUB_EQ"
                                                   "           Argument {"
                                                   "               Variable: \"counter\""
                                                   "           }"
                                                   "           Argument {"
                                                   "               Value: \"-1\""
                                                   "           }"
                                                   "       }"
                                                   "   }"
                                                   "   Action {"
                                                   "       SleepAction {"
                                                   "           NanoSeconds: 25000000" // 25 ms
                                                   "       }"
                                                   "   }"
                                                   "   Action {"
                                                   "       SleepAction {"
                                                   "           NanoSeconds: 25000000" // 25 ms
                                                   "       }"
                                                   "   }"
                                                   "}"));
            TInstant t0 = Now();
            for (size_t i = 0; i < 10; i++) {
                LWPROBE(Simplest);
            }
            TInstant t1 = Now();
            ui64 duration = (t1.NanoSeconds() - t0.NanoSeconds());
            Cout << "multiple sleep tested, expected 100000000 ns, measured " << duration << " ns" << Endl;
        }

        void SleepCheck(const TConfig& cfg) {
            TProbes p(cfg.UnsafeLWTrace);
            p.Mngr.New("test-sleep", MakeQuery(
                                         "Blocks {"
                                         "   ProbeDesc {"
                                         "       Name: \"Simplest\""
                                         "       Provider: \"LWTRACE_TESTS_PROVIDER\""
                                         "   }"
                                         "   Action {"
                                         "       SleepAction {"
                                         "           NanoSeconds: 100000000" // 100 ms
                                         "       }"
                                         "   }"
                                         "}"));
            TInstant t0 = Now();
            for (size_t i = 0; i < 10; i++) {
                LWPROBE(Simplest);
            }
            TInstant t1 = Now();
            ui64 duration = (t1.NanoSeconds() - t0.NanoSeconds()) / (ui64)10;
            Cout << "sleep tested, expected 100000000 ns, measured " << duration << " ns" << Endl;
        }

        void KillCheckChild(const TConfig& cfg, TPipeHandle& writer) {
            TProbes p(cfg.UnsafeLWTrace);
            p.Mngr.New("test-kill", MakeQuery(
                                        "Blocks {"
                                        "   ProbeDesc {"
                                        "       Name: \"Simplest\""
                                        "       Provider: \"LWTRACE_TESTS_PROVIDER\""
                                        "   }"
                                        "   Action {"
                                        "       KillAction {"
                                        "       }"
                                        "   }"
                                        "}"));
            // Send "i'm alive and ok" to the parent (0)
            char buffer = 0;
            writer.Write(&buffer, 1);
            LWPROBE(Simplest);
            // Send "i'm alive and that's not OK" to the parent (1)
            buffer = 1;
            writer.Write(&buffer, 1);
        }

        void KillCheckParent(TPipeHandle& reader) {
            char buffer = -1;
            reader.Read(&buffer, 1);
            reader.Read(&buffer, 1);
            if (buffer == -1)
                Cerr << "\t\terror: process died before transfering OK message during the KillAction test!" << Endl;
            else if (buffer != 0)
                Cerr << "\t\terror: process failed to die on time during the KillAction test!" << Endl;
            else
                Cout << "\t\tkill executor tested OK." << Endl;
        }

        void KillCheck(const TConfig& cfg) {
#ifdef _unix_
            TPipeHandle reader;
            TPipeHandle writer;
            TPipeHandle::Pipe(reader, writer);
            Cout << "forking the process..." << Endl;
            pid_t cpid = fork();
            if (cpid == -1) {
                Cerr << "\t\terror forking for the KillAction test!" << Endl;
            } else if (cpid == 0) {
                reader.Close();
                KillCheckChild(cfg, writer);
                writer.Close();
                exit(EXIT_SUCCESS);
            } else {
                writer.Close();
                KillCheckParent(reader);
                reader.Close();
            }
#else
            Cout << "kill action test for windows is not implemented." << Endl;
#endif
        }

        void LogIntModFilter(const TConfig& cfg) {
            TProbes p(cfg.UnsafeLWTrace);
            p.Mngr.New("test-trace", MakeQuery(
                                         "Blocks {"
                                         "    ProbeDesc {"
                                         "        Name: \"IntParam\""
                                         "        Provider: \"LWTRACE_TESTS_PROVIDER\""
                                         "    }"
                                         "    Predicate {"
                                         "        Operators {"
                                         "            Type: OT_GT"
                                         "            Argument {"
                                         "                Param: \"value\""
                                         "            }"
                                         "            Argument {"
                                         "                Value: \"0\""
                                         "            }"
                                         "        }"
                                         "    }"
                                         "    Action {"
                                         "        StatementAction {"
                                         "            Type: ST_ADD_EQ"
                                         "            Argument {"
                                         "                Variable: \"counter\""
                                         "            }"
                                         "            Argument {"
                                         "                Value: \"1\""
                                         "            }"
                                         "        }"
                                         "    }"
                                         "    Action {"
                                         "        StatementAction {"
                                         "            Type: ST_ADD"
                                         "            Argument {"
                                         "                Variable: \"counter\""
                                         "            }"
                                         "            Argument {"
                                         "                Variable: \"counter\""
                                         "            }"
                                         "            Argument {"
                                         "                Value: \"1\""
                                         "            }"
                                         "        }"
                                         "    }"
                                         "    Action {"
                                         "        StatementAction {"
                                         "            Type: ST_MOD"
                                         "            Argument {"
                                         "                Variable: \"counter\""
                                         "            }"
                                         "            Argument {"
                                         "                Variable: \"counter\""
                                         "            }"
                                         "            Argument {"
                                         "                Value: \"20\""
                                         "            }"
                                         "        }"
                                         "    }"
                                         "}"
                                         "Blocks {"
                                         "    ProbeDesc {"
                                         "        Name: \"IntParam\""
                                         "        Provider: \"LWTRACE_TESTS_PROVIDER\""
                                         "    }"
                                         "    Predicate {"
                                         "        Operators {"
                                         "            Type: OT_EQ"
                                         "            Argument {"
                                         "                Variable: \"counter\""
                                         "            }"
                                         "            Argument {"
                                         "                Value: \"0\""
                                         "            }"
                                         "        }"
                                         "        Operators {"
                                         "            Type: OT_GT"
                                         "            Argument {"
                                         "                Param: \"value\""
                                         "            }"
                                         "            Argument {"
                                         "                Value: \"0\""
                                         "            }"
                                         "        }"
                                         "    }"
                                         "    Action {"
                                         "        LogAction {"
                                         "            LogTimestamp: false"
                                         "        }"
                                         "    }"
                                         "}"));
            Cout << "call to probe with int mod filter (always true, mod 10) and log executors: " << p.IntParamTime(cfg) << Endl;
        }

#define FOR_EACH_TEST()                                \
    FOR_EACH_TEST_MACRO(LogIntModFilter)               \
    FOR_EACH_TEST_MACRO(SleepCheck)                    \
    FOR_EACH_TEST_MACRO(MultipleActionsCheck)          \
    FOR_EACH_TEST_MACRO(KillCheck)                     \
    FOR_EACH_TEST_MACRO(InMemoryLog)                   \
    FOR_EACH_TEST_MACRO(InMemoryLogCheck)              \
    FOR_EACH_TEST_MACRO(NoExec)                        \
    FOR_EACH_TEST_MACRO(Log)                           \
    FOR_EACH_TEST_MACRO(LogTs)                         \
    FOR_EACH_TEST_MACRO(FalseIntFilter)                \
    FOR_EACH_TEST_MACRO(LogIntAfterFilter)             \
    FOR_EACH_TEST_MACRO(LogInt)                        \
    FOR_EACH_TEST_MACRO(FalseStringFilter)             \
    FOR_EACH_TEST_MACRO(FalseStringFilterPartialMatch) \
    FOR_EACH_TEST_MACRO(LogStringAfterFilter)          \
    FOR_EACH_TEST_MACRO(LogString)                     \
    FOR_EACH_TEST_MACRO(LogSymbol)                     \
    FOR_EACH_TEST_MACRO(LogCheck)                      \
    /**/

        int Main(int argc, char** argv) {
            TConfig cfg;
            using namespace NLastGetopt;
            TOpts opts = NLastGetopt::TOpts::Default();
            opts.AddLongOption('c', "cycles", "cycles count").RequiredArgument("N").DefaultValue(ToString(cfg.Cycles)).StoreResult(&cfg.Cycles);
            opts.AddLongOption('r', "runs", "runs count").RequiredArgument("N").DefaultValue(ToString(cfg.Runs)).StoreResult(&cfg.Runs);
            opts.AddLongOption('u', "unsafe-lwtrace", "allow destructive actions").OptionalValue(ToString(true)).DefaultValue(ToString(false)).StoreResult(&cfg.UnsafeLWTrace);
            opts.AddHelpOption('h');
            TOptsParseResult res(&opts, argc, argv);

            TVector<TString> tests = res.GetFreeArgs();
            if (tests.empty()) {
#define FOR_EACH_TEST_MACRO(t) tests.push_back(#t);
                FOR_EACH_TEST()
#undef FOR_EACH_TEST_MACRO
            }
            for (size_t i = 0; i < tests.size(); i++) {
                const TString& test = tests[i];
#define FOR_EACH_TEST_MACRO(t) \
    if (test == #t) {          \
        Cout << #t ": \t";     \
        t(cfg);                \
    }
                FOR_EACH_TEST()
#undef FOR_EACH_TEST_MACRO
            }

            if (TCheck::ObjCount != 0) {
                Cout << ">>>>> THERE IS AN OBJECT LEAK <<<<<" << Endl;
                Cout << "NLWTrace::TCheck::ObjCount = " << TCheck::ObjCount << Endl;
            }

            Cout << "Done" << Endl;
            return 0;
        }

    }

}

template <>
void Out<NLWTrace::NTests::TMeasure>(IOutputStream& os, TTypeTraits<NLWTrace::NTests::TMeasure>::TFuncParam measure) {
    os << Sprintf("\n\t\t%.6lf +- %.6lf us,\tRPS: %30.3lf (%.1fM)", measure.Average, measure.Sigma, 1000000.0 / measure.Average, 1.0 / measure.Average);
}

int main(int argc, char** argv) {
    try {
        return NLWTrace::NTests::Main(argc, argv);
    } catch (std::exception& e) {
        Cerr << e.what() << Endl;
        return 1;
    } catch (...) {
        Cerr << "Unknown error" << Endl;
        return 1;
    }
}
