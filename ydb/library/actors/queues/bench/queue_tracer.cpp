#include "queue_tracer.h"


namespace NActors::NQueueBench::NTracing {


    void TStatsCollector::AddStats(const NActors::TStatsObserver::TStats &stats) {
        TGuard<TMutex> guard(Mutex);
        Stats += stats;
    }

    TString TStatsCollector::PrettyInt(ui64 value) {
        std::initializer_list<std::pair<char, ui64>> measures = {
            {'E', 1'000'000'000'000'000'000},
            {'P', 1'000'000'000'000'000},
            {'T', 1'000'000'000'000},
            {'G', 1'000'000'000},
            {'M', 1'000'000},
            {'k', 1'000},
        };
        for (auto &[prefix, val] : measures) {
            if (value >= val) {
                ui64 f = value / val;
                ui64 s = value % val / (val / 100);
                if (s) {
                    return TStringBuilder() << f << '.' << s << prefix;
                } else {
                    return TStringBuilder() << f << prefix;
                }
            }
        }
        return ToString(value);
    }

    TString TStatsCollector::PrettyDivision(ui64 value, ui64 divider) {
        if (!value) {
            return "0";
        }
        if (!divider) {
            return "inf";
        }
        if (100 * divider > value) {
            return ToString(double(value) / divider);
        }
        return PrettyInt(value / divider);
    }

    TString TStatsCollector::Pretty(ui64 stat, ui64 seconds, ui64  threads) {
        TStringBuilder pretty;
        pretty << PrettyInt(stat);
        if (seconds) {
            pretty << " (" << PrettyDivision(stat, seconds) << " per second)";
        }
        if (threads) {
            pretty << " (" << PrettyDivision(stat, threads) << " per thread)";
        }
        if (seconds && threads) {
            pretty << " (" << PrettyDivision(stat, seconds * threads) << " per thread per second)";
        }
        return pretty;
    }

    void TStatsCollector::GeneralPrint(ui64 seconds, ui64 producerThreads, ui64 consumerThreads, bool shortOutput) const {

#define IF_ALLOWED(value) \
    if (!shortOutput || value) \
// IF_ALLOWED
#define PRINT_STAT(STAT_NAME, TAB, THREAD_COUNT) \
    IF_ALLOWED(Stats. STAT_NAME) Cerr << TAB << #STAT_NAME ": " << Pretty(Stats. STAT_NAME, seconds, THREAD_COUNT) << Endl; \
// PRINT_STAT
        PRINT_STAT(FoundOldSlotInFastPush, "", producerThreads);
        PRINT_STAT(FoundOldSlotInSlowPush, "", producerThreads);
        PRINT_STAT(FoundOldSlotInSlowPop, "", consumerThreads);
        PRINT_STAT(FoundOldSlotInFastPop, "", consumerThreads);
        PRINT_STAT(FoundOldSlotInReallyFastPop, "", consumerThreads);
        PRINT_STAT(FailedPush, "", producerThreads);

        PRINT_STAT(SuccessPush, "", producerThreads);
        PRINT_STAT(SuccessFastPush, "  ", producerThreads);
        PRINT_STAT(SuccessSlowPush, "  ", producerThreads);
        PRINT_STAT(SuccessPop, "", consumerThreads);
        PRINT_STAT(SuccessReallyFastPop, "  ", consumerThreads);
        PRINT_STAT(SuccessFastPop, "  ", consumerThreads);
        PRINT_STAT(SuccessSlowPop, "  ", consumerThreads);
        PRINT_STAT(SuccessOvertakenPop, "  ", consumerThreads);
#undef PRINT_STAT

#define PRINT_LONG(STAT_PREFIX, THREAD_COUNT) \
    IF_ALLOWED(Stats. STAT_PREFIX ## 10It + Stats. STAT_PREFIX ## 100It + Stats. STAT_PREFIX ## 100It)  Cerr << #STAT_PREFIX << ": " << Endl; \
    IF_ALLOWED(Stats. STAT_PREFIX ## 10It) Cerr << "  10It: " << Pretty(Stats. STAT_PREFIX ## 10It - Stats. STAT_PREFIX ## 100It - Stats. STAT_PREFIX ## 1000It, seconds, THREAD_COUNT) << Endl; \
    IF_ALLOWED(Stats. STAT_PREFIX ## 100It) Cerr << "  100It: " << Pretty(Stats. STAT_PREFIX ## 100It - Stats. STAT_PREFIX ## 1000It, seconds, THREAD_COUNT) << Endl; \
    IF_ALLOWED(Stats. STAT_PREFIX ## 1000It) Cerr << "  1000It: " << Pretty(Stats. STAT_PREFIX ## 1000It, seconds, THREAD_COUNT) << Endl; \
// PRINT_LONG
        PRINT_LONG(LongPush, producerThreads);
        PRINT_LONG(LongSlowPop, consumerThreads);
        PRINT_LONG(LongFastPop, consumerThreads);
        PRINT_LONG(LongReallyFastPop, consumerThreads);
#undef PRINT_LONG
    }

    void TStatsCollector::Print(ui64 seconds, ui64 producerThreads, ui64 consumerThreads, bool shortOutput) const {
        Cerr << "DurationInSeconds: " << seconds << Endl;
        Cerr << "ProducerThreads: " << producerThreads << Endl;
        Cerr << "ConsumerThreads: " << consumerThreads << Endl;
        GeneralPrint(seconds, producerThreads, consumerThreads, shortOutput);
    }

    void TStatsCollector::Print(ui64 seconds, ui64 threads, bool shortOutput) const {
        Cerr << "DurationInSeconds: " << seconds << Endl;
        Cerr << "Threads: " << threads << Endl;
        GeneralPrint(seconds, threads, threads, shortOutput);
    }
}
