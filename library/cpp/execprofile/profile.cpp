#include <util/system/defaults.h>

#include "profile.h"

#if defined(_unix_) && !defined(_bionic_) && !defined(_cygwin_)

#include <signal.h>
#include <sys/time.h>
#include <sys/resource.h>
#if defined(_darwin_)
#include <sys/ucontext.h>
#else
#include <ucontext.h>
#endif
#include <dlfcn.h>

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/system/platform.h>
#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/noncopyable.h>
#include <util/generic/algorithm.h>
#include <util/generic/vector.h>
#include <util/stream/file.h>
#include <util/string/util.h>
#include <util/system/datetime.h>

// This class sets SIGPROF handler and captures instruction pointer in it.
class TExecutionSampler : TNonCopyable {
public:
    typedef TVector<std::pair<void*, size_t>> TSampleVector;

    struct TStats {
        ui64 SavedSamples;
        ui64 DroppedSamples;
        ui64 SearchSkipCount;
    };

    // NOTE: There is no synchronization here as the instance is supposed to be
    // created on the main thread.
    static TExecutionSampler* Instance() {
        if (SInstance == nullptr) {
            SInstance = new TExecutionSampler();
        }

        return SInstance;
    }

    void Start() {
        // Set signal handler
        struct sigaction sa;
        sa.sa_sigaction = ProfilerSignalHandler;
        sigemptyset(&sa.sa_mask);
        sa.sa_flags = SA_SIGINFO;
        if (sigaction(SIGPROF, &sa, &OldSignalHandler) != 0)
            return;

        // Set interval timer
        itimerval tv;
        tv.it_interval.tv_sec = tv.it_value.tv_sec = 0;
        tv.it_interval.tv_usec = tv.it_value.tv_usec = SAMPLE_INTERVAL;
        setitimer(ITIMER_PROF, &tv, &OldTimerValue);

        Started = true;
    }

    void Stop(TSampleVector& sampleVector, TStats& stats) {
        // Reset signal handler and timer
        if (Started) {
            setitimer(ITIMER_PROF, &OldTimerValue, nullptr);
            sleep(1);
        }

        WaitForWriteFlag();

        if (Started) {
            sigaction(SIGPROF, &OldSignalHandler, nullptr);
            Started = false;
        }

        TExecutionSampler::TSampleVector hits;
        hits.reserve(Samples);
        for (size_t i = 0; i < SZ; ++i) {
            if (Ips[i].first != nullptr) {
                hits.push_back(Ips[i]);
            }
        }
        stats.SavedSamples = Samples;
        stats.DroppedSamples = AtomicGet(DroppedSamples);
        stats.SearchSkipCount = SearchSkipCount;
        AtomicUnlock(&WriteFlag);

        Sort(hits.begin(), hits.end(), TCompareFirst());

        sampleVector.swap(hits);
    }

    void ResetStats() {
        WaitForWriteFlag();
        Clear();
        AtomicUnlock(&WriteFlag);
    }

private:
    static const size_t SZ = 2 * 1024 * 1024; // size of the hash table
                                              // inserts work faster if it's a power of 2
    static const int SAMPLE_INTERVAL = 1000;  // in microseconds

    struct TCompareFirst {
        bool operator()(const std::pair<void*, size_t>& a, const std::pair<void*, size_t>& b) const {
            return a.first < b.first;
        }
    };

    TExecutionSampler()
        : Started(false)
        , Ips(SZ)
        , WriteFlag(0)
        , DroppedSamples(0)
        , Samples(0)
        , UniqueSamples(0)
        , SearchSkipCount(0)
    {
    }

    ~TExecutionSampler() = default;

    // Signal handler is not allowed to do anything that can deadlock with activity
    // on the thread to which the signal is delivered or corrupt data structures that
    // were in process of update.
    // One such thing is memory allocation. That's why a fixed size vector is
    // preallocated at start.
    static void ProfilerSignalHandler(int signal, siginfo_t* info, void* context) {
        (void)info;
        if (signal != SIGPROF) {
            return;
        }

        ucontext_t* ucontext = reinterpret_cast<ucontext_t*>(context);
        Y_ASSERT(SInstance != nullptr);

        SInstance->CaptureIP(GetIp(&ucontext->uc_mcontext));
    }

    void WaitForWriteFlag() {
        // Wait for write flag to be reset
        ui32 delay = 100;
        while (!AtomicTryLock(&WriteFlag)) {
            usleep(delay);
            delay += delay;
            delay = Min(delay, (ui32)5000);
        }
    }

    void CaptureIP(void* rip) {
        // Check if the handler on another thread is in the process of adding a sample
        // If this is the case, we just drop the current sample as this should happen
        // rarely.
        if (AtomicTryLock(&WriteFlag)) {
            AddSample(rip);
            AtomicUnlock(&WriteFlag);
        } else {
            AtomicIncrement(DroppedSamples);
        }
    }

    // Hash function applied to the addresses
    static inline ui32 Hash(void* key) {
        return ((size_t)key + (size_t)key / SZ) % SZ;
    }

    // Get instruction pointer from the context
    static inline void* GetIp(const mcontext_t* mctx) {
#if defined _freebsd_
#if defined _64_
        return (void*)mctx->mc_rip;
#else
        return (void*)mctx->mc_eip;
#endif
#elif defined _linux_
#if defined _64_
#if defined(_arm_)
        return (void*)mctx->pc;
#else
        return (void*)mctx->gregs[REG_RIP];
#endif
#else
        return (void*)mctx->gregs[REG_EIP];
#endif
#elif defined _darwin_
#if defined _64_
#if defined(_arm_)
        return (void*)(*mctx)->__ss.__pc;
#else
        return (void*)(*mctx)->__ss.__rip;
#endif
#else
#if defined(__IOS__)
        return (void*)(*mctx)->__ss.__pc;
#else
        return (void*)(*mctx)->__ss.__eip;
#endif
#endif
#endif
    }

    inline bool AddSample(void* key) {
        ui32 slot = Hash(key);
        ui32 prevSlot = (slot - 1) % SZ;

        while (key != Ips[slot].first && !IsSlotEmpty(slot) && slot != prevSlot) {
            slot = (slot + 1) % SZ;
            SearchSkipCount++;
        }

        if (key == Ips[slot].first) {
            // increment the count
            Ips[slot].second++;
            ++Samples;
        } else if (InsertsAllowed()) {
            // add new sample and set the count to 1
            Ips[slot].first = key;
            Ips[slot].second = 1;
            ++UniqueSamples;
            ++Samples;
        } else {
            // don't insert new sample if the search is becoming too slow
            AtomicIncrement(DroppedSamples);
            return false;
        }

        return true;
    }

    inline bool IsSlotEmpty(ui32 slot) const {
        return Ips[slot].first == nullptr;
    }

    inline bool InsertsAllowed() const {
        return UniqueSamples < SZ / 2;
    }

    void
    Clear() {
        Y_ASSERT(WriteFlag == 1);

        for (size_t i = 0; i < SZ; ++i) {
            Ips[i] = std::make_pair((void*)nullptr, (size_t)0);
        }
        Samples = 0;
        AtomicSet(DroppedSamples, 0);
        UniqueSamples = 0;
        SearchSkipCount = 0;
    }

    bool Started;
    struct sigaction OldSignalHandler;
    itimerval OldTimerValue;

    TVector<std::pair<void*, size_t>>
        Ips; // The hash table storing addresses and their hitcounts

    // TODO: on a big multiproc cache line false sharing by the flag and count might become an issue
    TAtomic WriteFlag;      // Is used to syncronize access to the hash table
    TAtomic DroppedSamples; // "dropped sample" count will show how many times
                            // a sample was dropped either because of write conflict
                            // or because of the hash table had become too filled up
    ui64 Samples;           // Saved samples count
    ui64 UniqueSamples;     // Number of unique addresses
    ui64 SearchSkipCount;   // Total number of linear hash table probes due to collisions

    static TExecutionSampler* SInstance;
};

// Performs analysis of samples captured by TExecutionSampler
class TSampleAnalyser : TNonCopyable {
public:
    TSampleAnalyser(TExecutionSampler::TSampleVector& samples, const TExecutionSampler::TStats& stats, bool putTimeStamps = false)
        : Samples()
        , Stats(stats)
        , PutTimestamps(putTimeStamps)
    {
        Samples.swap(samples);
    }

    ~TSampleAnalyser() = default;

    void Analyze(FILE* out) const;

private:
    TExecutionSampler::TSampleVector Samples;
    TExecutionSampler::TStats Stats;
    bool PutTimestamps;
};

void TSampleAnalyser::Analyze(FILE* out) const {
    fprintf(out, "samples: %" PRIu64 "    unique: %" PRIu64 "   dropped:  %" PRIu64 "   searchskips:   %" PRIu64 "\n",
            (ui64)Stats.SavedSamples, (ui64)Samples.size(),
            (ui64)Stats.DroppedSamples, (ui64)Stats.SearchSkipCount);

    fprintf(out, "\nSamples:\n");
    size_t funcCnt = 0;
    void* prevModBase = (void*)-1;
    void* prevFunc = (void*)-1;
    for (size_t i = 0; i < Samples.size(); ++i) {
        // print cycle count once in a while to estimate time consumed by
        // dumping the samples
        if (PutTimestamps && (i % 1000 == 0)) {
            ui64 tm = GetCycleCount();
            fprintf(out, "TM: %" PRIu64 "\n", tm);
        }

        Dl_info addrInfo;
        if (dladdr(Samples[i].first, &addrInfo)) {
            if (addrInfo.dli_fbase != prevModBase || addrInfo.dli_saddr != prevFunc) {
                fprintf(out, "Func\t%" PRISZT "\t%p\t%p\t%s\t%s\n",
                        funcCnt,
                        addrInfo.dli_fbase,
                        addrInfo.dli_saddr,
                        addrInfo.dli_fname,
                        addrInfo.dli_sname);
                prevModBase = addrInfo.dli_fbase;
                prevFunc = addrInfo.dli_saddr;
                ++funcCnt;
            }
        } else {
            fprintf(out, "[dladdr failed]\n");
        }
        fprintf(out, "%" PRISZT "\t%p\t%lu\n", i, Samples[i].first, Samples[i].second);
    }
}

TExecutionSampler* TExecutionSampler::SInstance = nullptr;

// Starts capturing execution samples
void BeginProfiling() {
    TExecutionSampler::Instance()->Start();
}

// Resets captured execution samples
void ResetProfile() {
    TExecutionSampler::Instance()->ResetStats();
}

void DumpRUsage(FILE* out) {
    rusage ru;
    int e = getrusage(RUSAGE_SELF, &ru);
    if (e != 0)
        return;

    fprintf(out,
            "user time:                       %lf\n"
            "system time:                     %lf\n"
            "max RSS:                         %ld\n"
            "shared text:                     %ld\n"
            "unshared data:                   %ld\n"
            "unshared stack:                  %ld\n"
            "page reclaims:                   %ld\n"
            "page faults:                     %ld\n"
            "swaps:                           %ld\n"
            "block input ops:                 %ld\n"
            "block output ops:                %ld\n"
            "msg sent:                        %ld\n"
            "msg received:                    %ld\n"
            "signals:                         %ld\n"
            "voluntary ctx switches:          %ld\n"
            "involuntary ctx switches:        %ld\n\n",
            ru.ru_utime.tv_sec + (ru.ru_utime.tv_usec * 0.000001),
            ru.ru_stime.tv_sec + (ru.ru_stime.tv_usec * 0.000001),
            ru.ru_maxrss, ru.ru_ixrss, ru.ru_idrss, ru.ru_isrss,
            ru.ru_minflt, ru.ru_majflt, ru.ru_nswap,
            ru.ru_inblock, ru.ru_oublock,
            ru.ru_msgsnd, ru.ru_msgrcv,
            ru.ru_nsignals,
            ru.ru_nvcsw, ru.ru_nivcsw);
}

// Pauses capturing execution samples and dumps them to the file
// Samples are not cleared so that profiling can be continued by calling BeginProfiling()
// or it can be started from scratch by calling ResetProfile() and then BeginProfiling()
void EndProfiling(FILE* out) {
    DumpRUsage(out);

    TExecutionSampler::TSampleVector samples;
    TExecutionSampler::TStats stats;
    TExecutionSampler::Instance()->Stop(samples, stats);

    TSampleAnalyser analyzer(samples, stats);
    analyzer.Analyze(out);
}

void EndProfiling() {
    static unsigned cnt = 0;
    char nameBuf[256];
    snprintf(nameBuf, sizeof(nameBuf), "./%s.%d.%u.profile", getprogname(), (int)getpid(), cnt);
    FILE* out = fopen(nameBuf, "a");
    EndProfiling(out);
    fclose(out);
    ++cnt;
}

#else

// NOTE: not supported on Windows

void BeginProfiling() {
}

void ResetProfile() {
}

void EndProfiling(FILE*) {
}

void EndProfiling() {
}

#endif
