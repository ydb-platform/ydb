#include "tcmalloc.h"

#include <contrib/libs/tcmalloc/tcmalloc/malloc_extension.h>

#include <ydb/library/actors/prof/tag.h>
#include <library/cpp/cache/cache.h>
#include <library/cpp/html/pcdata/pcdata.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <ydb/core/mon/mon.h>

#include <util/stream/format.h>

using namespace NActors;

namespace NKikimr {

using TDynamicCountersPtr = TIntrusivePtr<::NMonitoring::TDynamicCounters>;
using TDynamicCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;
using THistogramPtr = NMonitoring::THistogramPtr;

static void FormatPrettyNumber(IOutputStream& out, ssize_t val) {
    if (val == 0) {
        out << "0";
        return;
    }
    if (val < 0) {
        out << "- ";
    }
    ui64 value = val < 0 ? -val : val;
    TSmallVec<ui64> parts;
    while (value > 0) {
        auto head = value / 1000;
        parts.push_back(value - head * 1000);
        value = head;
    }
    out << parts.back();
    parts.pop_back();
    while (!parts.empty()) {
        out << Sprintf(" %03" PRIu64, parts.back());
        parts.pop_back();
    }
}

static void FormatSize(IOutputStream& out, size_t size) {
    if (size < (1 << 10)) {
        out << size;
    } else if (size < (1 << 20)) {
        out << size << " ("
            << Sprintf("%.1f", size / float(1 << 10)) << " KiB)";
    } else {
        out << size << " ("
            << Sprintf("%.1f", size / float(1 << 20)) << " MiB)";
    }
}

static ui64 GetProperty(
    const std::map<std::string, tcmalloc::MallocExtension::Property>& properties,
    const char* name)
{
    auto found = properties.find(name);
    return found != properties.end() ? found->second.value : 0;
}

static ui64 GetCachesSize(
    const std::map<std::string, tcmalloc::MallocExtension::Property>& properties)
{
    return
        GetProperty(properties, "tcmalloc.page_heap_free") +
        GetProperty(properties, "tcmalloc.central_cache_free") +
        GetProperty(properties, "tcmalloc.transfer_cache_free") +
        GetProperty(properties, "tcmalloc.sharded_transfer_cache_free") +
        GetProperty(properties, "tcmalloc.cpu_free") +
        GetProperty(properties, "tcmalloc.thread_cache_free");
}

struct TAllocationStats {
    size_t Size;
    size_t Count;
    size_t RequestedSize;

    static constexpr size_t MaxSizeIndex = 32;
    size_t Sizes[MaxSizeIndex];
    size_t Counts[MaxSizeIndex];

    static constexpr ui32 MaxTag = 2048;
    size_t TagSizes[MaxTag];
    size_t TagCounts[MaxTag];

    TAllocationStats() {
        Zero(*this);
    }

    static size_t SizeUpperBound(size_t index) {
        return 1 << std::min(MaxSizeIndex - 1, index);
    }

    static size_t GetIndex(size_t size) {
        size_t index = 0;
        if (size > 0) {
            index = std::min(MaxSizeIndex - 1, (size_t)GetValueBitCount(size - 1));
        }
        return index;
    }

    static const char* GetTagName(ui32 tag) {
        const char* name = NProfiling::GetTag(tag);
        return name ? name : "_DEFAULT";
    }

    void Add(const tcmalloc::Profile::Sample& sample) {
        Size += sample.sum;
        Count += sample.count;
        RequestedSize += sample.count * sample.requested_size;

        auto index = GetIndex(sample.allocated_size);
        Sizes[index] += sample.sum;
        Counts[index] += sample.count;

        ui32 tag = reinterpret_cast<uintptr_t>(sample.user_data);
        tag = (tag < MaxTag) ? tag : NProfiling::GetOverlimitCountersTag();
        TagSizes[tag] += sample.sum;
        TagCounts[tag] += sample.count;
    }

    void DumpSizeStats(IOutputStream& out, const char* marker, const char* sep) const {
        out << marker << sep << sep;
        for (size_t b = 0; b < MaxSizeIndex; ++b) {
            if (b == MaxSizeIndex - 1) {
                out << "inf          : ";
            } else {
                out << "<= " << Sprintf("%-10" PRIu64, SizeUpperBound(b)) << ": ";
            }
            if (!Sizes[b]) {
                out << "0" << sep;
                continue;
            }
            out << "size ";
            FormatSize(out, Sizes[b]);
            out << ", count " << Counts[b] << sep;
        }
        out << Endl;
    }

    void DumpTagStats(IOutputStream& out, const char* marker, const char* sep) const {
        out << marker << sep << sep;
        for (ui32 t = 0; t < MaxTag; ++t) {
            if (!TagSizes[t]) {
                continue;
            }
            out << Sprintf("%-32s", TAllocationStats::GetTagName(t)) << ": size ";
            FormatSize(out, TagSizes[t]);
            out << ", count " << TagCounts[t] << sep;
        }
        out << Endl;
    }
};


class TAllocationAnalyzer {
    const tcmalloc::Profile Profile;

    struct TSymbol {
        const void* Address;
        TString Name;
    };
    TLFUCache<void*, TSymbol> SymbolCache;

    struct TStackKey {
        int Depth;
        void* Stack[tcmalloc::Profile::Sample::kMaxStackDepth];

        explicit TStackKey(const tcmalloc::Profile::Sample& sample) {
            Depth = std::min(sample.depth, tcmalloc::Profile::Sample::kMaxStackDepth);
            std::memcpy(Stack, sample.stack, Depth * sizeof(void*));
        }

        bool operator==(const TStackKey& key) const {
            if (Depth != key.Depth) {
                return false;
            }
            return std::memcmp(Stack, key.Stack, Depth * sizeof(void*)) == 0;
        }

        struct THash {
            size_t operator()(const TStackKey& key) const {
                std::string_view view((char*)key.Stack, key.Depth * sizeof(void*));
                return std::hash<std::string_view>()(view);
            }
        };
    };

    struct TStackStats {
        std::vector<tcmalloc::Profile::Sample> Samples;
        size_t Size = 0;
        size_t Count = 0;
        ui32 Tag = 0;
    };

    std::unordered_map<TStackKey, TStackStats, TStackKey::THash> Stacks;
    size_t SampleCount = 0;
    size_t Size = 0;
    size_t Count = 0;
    size_t RequestedSize = 0;

    bool Prepared = false;

private:
    void PrintBackTrace(IOutputStream& out, void* const* stack, size_t sz,
        const char* sep)
    {
        char name[1024];
        for (size_t i = 0; i < sz; ++i) {
            TSymbol symbol;
            auto it = SymbolCache.Find(stack[i]);
            if (it != SymbolCache.End()) {
                symbol = it.Value();
            } else {
                TResolvedSymbol rs = ResolveSymbol(stack[i], name, sizeof(name));
                symbol = {rs.NearestSymbol, rs.Name};
                SymbolCache.Insert(stack[i], symbol);
            }

            out << Hex((intptr_t)stack[i], HF_FULL) << " " << symbol.Name;
            intptr_t offset = (intptr_t)stack[i] - (intptr_t)symbol.Address;
            if (offset)
                out << " +" << offset;
            out << sep;
        }
    }

    void PrintSample(IOutputStream& out, const tcmalloc::Profile::Sample* sample,
        const char* sep) const
    {
        out << sample->sum << " = "
            << sample->count << " * "
            << sample->allocated_size << " ("
            << sample->requested_size << ")"
            << sep;
    }

    void PrintStack(IOutputStream& out, const TStackStats* stats, size_t sampleCountLimit,
        const char* marker, const char* sep)
    {
        std::vector<const tcmalloc::Profile::Sample*> samples;
        samples.reserve(stats->Samples.size());
        size_t size = 0;
        size_t count = 0;

        for (const auto& sample : stats->Samples) {
            samples.push_back(&sample);
            size += sample.sum;
            count += sample.count;
        }

        std::sort(samples.begin(), samples.end(), [] (const auto* l, const auto* r) {
            return l->sum > r->sum;
        });

        out << marker << sep << sep << "size ";
        FormatSize(out, size);
        out << ", count " << count
            << ", " << TAllocationStats::GetTagName(stats->Tag) << sep;

        size_t i = 0;
        for (const auto* sample : samples) {
            PrintSample(out, sample, sep);
            if (++i >= sampleCountLimit) {
                break;
            }
        }

        if (samples.size() > sampleCountLimit) {
            out << "....TRUNCATED, first " << sampleCountLimit << " samples are shown" << sep;
        }
        out << sep;

        if (samples.size()) {
            const auto& sample = samples.front();
            PrintBackTrace(out, sample->stack, sample->depth, sep);
        }
        out << Endl;
    }

    void PrintAllocatorDetails(IOutputStream& out, const char* sep) const {
        const auto properties = tcmalloc::MallocExtension::GetProperties();
        out << sep
            << "Physical memory used: ";
        FormatSize(out, GetProperty(properties, "generic.physical_memory_used"));
        out << sep
            << "  Application: ";
        FormatSize(out, GetProperty(properties, "generic.bytes_in_use_by_app"));
        out << sep
            << "  Allocator caches: ";
        FormatSize(out, GetCachesSize(properties));
        out << sep
            << "  Allocator metadata: ";
        FormatSize(out, GetProperty(properties, "tcmalloc.metadata_bytes"));
        out << sep;
    }

public:
    explicit TAllocationAnalyzer(tcmalloc::Profile&& profile)
        : Profile(std::move(profile))
        , SymbolCache(2048)
    {}

    void Prepare(TAllocationStats* allocationStats) {
        Y_ABORT_UNLESS(!Prepared);

        TAllocationStats allocations;

        Profile.Iterate([&] (const tcmalloc::Profile::Sample& sample) {
            if (!sample.sum) {
                return;
            }

            TStackKey key(sample);
            auto& stats = Stacks[key];
            stats.Samples.push_back(sample);
            stats.Size += sample.sum;
            stats.Count += sample.count;
            stats.Tag = reinterpret_cast<uintptr_t>(sample.user_data);

            allocations.Add(sample);

            ++SampleCount;
        });

        Size = allocations.Size;
        Count = allocations.Count;
        RequestedSize = allocations.RequestedSize;

        if (allocationStats) {
            *allocationStats = allocations;
        }

        Prepared = true;
    }

    void Dump(IOutputStream& out, size_t stackCountLimit, size_t sampleCountLimit,
        bool isHeap, bool forLog)
    {
        Y_ABORT_UNLESS(Prepared);

        const char* marker = forLog ?
            (isHeap ? "HEAP" : "PROFILE") : "--------------------------------";
        const char* sep = forLog ? " | " : "\n";

        out << marker << sep << sep;
        out << "Stack count: " << Stacks.size() << sep
            << "Sample count: " << SampleCount << sep
            << "Allocated bytes: ";
        FormatSize(out, Size);
        out << sep
            << "Allocation count: " << Count << sep;

        if (isHeap) {
            out << sep
                << "Requested bytes: ";
            FormatSize(out, RequestedSize);
            out << sep
                << "Wasted bytes: ";
            FormatSize(out, Size - RequestedSize);
            out << sep;

            PrintAllocatorDetails(out, sep);
        }

        if (!forLog) {
            out << sep
                << "Details: size = count * allocated_size (requested_size)" << sep;
        }

        out << Endl;

        std::vector<const TStackStats*> stats;
        stats.reserve(Stacks.size());
        for (auto& [key, stackStats] : Stacks) {
            stats.push_back(&stackStats);
        }

        std::sort(stats.begin(), stats.end(), [] (const auto* l, const auto* r) {
            return l->Size > r->Size;
        });

        size_t i = 0;
        for (const auto* stackStats : stats) {
            PrintStack(out, stackStats, sampleCountLimit, marker, sep);
            if (++i >= stackCountLimit) {
                break;
            }
        }

        if (stats.size() > stackCountLimit) {
            out << "....TRUNCATED, first " << stackCountLimit << " stacks are shown" << sep;
        }
        out << Endl;
    }
};


class TTcMallocStats : public IAllocStats {
    TDynamicCountersPtr CounterGroup;

    static constexpr const char* Names[] = {
        "generic.virtual_memory_used",
        "generic.physical_memory_used",
        "generic.bytes_in_use_by_app",
        "tcmalloc.page_heap_free",
        "tcmalloc.metadata_bytes",
        "tcmalloc.thread_cache_count",
        "tcmalloc.central_cache_free",
        "tcmalloc.transfer_cache_free",
        "tcmalloc.sharded_transfer_cache_free",
        "tcmalloc.cpu_free",
        "tcmalloc.per_cpu_caches_active",
        "tcmalloc.thread_cache_free",
        "tcmalloc.page_heap_unmapped",
    };

    std::unordered_map<TString, TDynamicCounterPtr> Counters;

    TDynamicCounterPtr CachesSize;

public:
    explicit TTcMallocStats(TDynamicCountersPtr group) {
        CounterGroup = group->GetSubgroup("component", "tcmalloc");
        for (auto name : Names) {
            Counters[name] = CounterGroup->GetCounter(name, false);
        }
        CachesSize = CounterGroup->GetCounter("tcmalloc.caches_bytes", false);
    }

    void Update() override {
        auto properties = tcmalloc::MallocExtension::GetProperties();
        for (auto name : Names) {
            if (auto it = properties.find(name); it != properties.end()) {
                Counters[name]->Set(it->second.value);
            }
        }
        CachesSize->Set(GetCachesSize(properties));
    }
};


class TTcMallocState : public IAllocState {
public:
    ui64 GetAllocatedMemoryEstimate() const override {
        const auto properties = tcmalloc::MallocExtension::GetProperties();

        ui64 used = GetProperty(properties, "generic.physical_memory_used");
        ui64 caches = GetCachesSize(properties);

        return used > caches ? used - caches : 0;
    }
};


class TTcMallocMonitor : public IAllocMonitor {
    TDynamicCountersPtr CounterGroup;

    THistogramPtr SizeHistogram;
    THistogramPtr CountHistogram;
    std::unordered_map<ui32, std::pair<TDynamicCounterPtr, TDynamicCounterPtr>> TaggedCounters;

    struct TControls {
        static constexpr size_t MaxSamplingRate = 4ll << 30;
        static constexpr size_t MaxPageCacheTargetSize = 128ll << 30;
        static constexpr size_t MaxPageCacheReleaseRate = 128ll << 20;

        static constexpr size_t DefaultPageCacheTargetSize = 512ll << 20;
        static constexpr size_t DefaultPageCacheReleaseRate = 8ll << 20;

        TControlWrapper ProfileSamplingRate;
        TControlWrapper GuardedSamplingRate;
        TControlWrapper MemoryLimit;
        TControlWrapper PageCacheTargetSize;
        TControlWrapper PageCacheReleaseRate;

        TControls()
            : ProfileSamplingRate(tcmalloc::MallocExtension::GetProfileSamplingRate(),
                64 << 10, MaxSamplingRate)
            , GuardedSamplingRate(MaxSamplingRate,
                64 << 10, MaxSamplingRate)
            , MemoryLimit(0,
                0, std::numeric_limits<i64>::max())
            , PageCacheTargetSize(DefaultPageCacheTargetSize,
                0, MaxPageCacheTargetSize)
            , PageCacheReleaseRate(DefaultPageCacheReleaseRate,
                0, MaxPageCacheReleaseRate)
        {}

        void Register(TIntrusivePtr<TControlBoard> icb) {
            icb->RegisterSharedControl(ProfileSamplingRate, "TCMallocControls.ProfileSamplingRate");
            icb->RegisterSharedControl(GuardedSamplingRate, "TCMallocControls.GuardedSamplingRate");
            icb->RegisterSharedControl(MemoryLimit, "TCMallocControls.MemoryLimit");
            icb->RegisterSharedControl(PageCacheTargetSize, "TCMallocControls.PageCacheTargetSize");
            icb->RegisterSharedControl(PageCacheReleaseRate, "TCMallocControls.PageCacheReleaseRate");
        }
    };
    TControls Controls;

private:
    void UpdateCounters() {
        auto profile = tcmalloc::MallocExtension::SnapshotCurrent(tcmalloc::ProfileType::kHeap);

        TAllocationAnalyzer analyzer(std::move(profile));
        TAllocationStats allocationStats;
        analyzer.Prepare(&allocationStats);

        auto updateHistogram = [] (THistogramPtr histogram, size_t* numbers) {
            histogram->Reset();
            auto snapshot = histogram->Snapshot();
            auto count = std::min(snapshot->Count(), (ui32)TAllocationStats::MaxSizeIndex);
            for (ui32 b = 0; b < count; ++b) {
                histogram->Collect(snapshot->UpperBound(b), numbers[b]);
            }
        };

        updateHistogram(SizeHistogram, allocationStats.Sizes);
        updateHistogram(CountHistogram, allocationStats.Counts);

        for (ui32 t = 0; t < TAllocationStats::MaxTag; ++t) {
            auto found = TaggedCounters.find(t);
            if (!allocationStats.TagSizes[t] && found == TaggedCounters.end()) {
                continue;
            }

            TDynamicCounterPtr size, count;

            if (found == TaggedCounters.end()) {
                auto name = TAllocationStats::GetTagName(t);
                auto group = CounterGroup->GetSubgroup("activity", name);
                size = group->GetCounter("tcmalloc.sampled_size_by_activity", false);
                count = group->GetCounter("tcmalloc.sampled_count_by_activity", false);
                TaggedCounters.emplace(t, std::make_pair(size, count));
            } else {
                size = found->second.first;
                count = found->second.second;
            }

            size->Set(allocationStats.TagSizes[t]);
            count->Set(allocationStats.TagCounts[t]);
        }
    }

    void UpdateControls() {
        tcmalloc::MallocExtension::SetProfileSamplingRate(Controls.ProfileSamplingRate);

        if (Controls.GuardedSamplingRate != TControls::MaxSamplingRate) {
            tcmalloc::MallocExtension::ActivateGuardedSampling();
        }
        tcmalloc::MallocExtension::SetGuardedSamplingRate(Controls.GuardedSamplingRate);

        tcmalloc::MallocExtension::MemoryLimit limit;
        limit.hard = false;
        limit.limit = Controls.MemoryLimit ?
            (size_t)Controls.MemoryLimit : std::numeric_limits<size_t>::max();
        tcmalloc::MallocExtension::SetMemoryLimit(limit);
    }

    void ReleaseMemoryIfNecessary(TDuration interval) {
        auto properties = tcmalloc::MallocExtension::GetProperties();
        i64 pageHeapSize = GetProperty(properties, "tcmalloc.page_heap_free");
        if (pageHeapSize > Controls.PageCacheTargetSize) {
            auto excess = (ui64)(pageHeapSize - Controls.PageCacheTargetSize);
            auto releaseLimit = (ui64)(Controls.PageCacheReleaseRate * interval.Seconds());
            tcmalloc::MallocExtension::ReleaseMemoryToSystem(
                std::min(excess, releaseLimit));
        }
    }

    void DumpCurrent(IOutputStream& out, tcmalloc::ProfileType type,
        size_t stackCountLimit = 256, size_t sampleCountLimit = 1024, bool forLog = false)
    {
        auto start = TInstant::Now();
        auto profile = tcmalloc::MallocExtension::SnapshotCurrent(type);
        auto end = TInstant::Now();

        TAllocationAnalyzer analyzer(std::move(profile));
        TAllocationStats allocationStats;
        analyzer.Prepare(&allocationStats);

        bool isHeap = (type == tcmalloc::ProfileType::kHeap);

        if (forLog) {
            const char* marker = "HEAP";
            const char* sep = " | ";
            out << marker << sep
                << "Snapshot calculation time: " << (end - start).MicroSeconds() << " us" << sep
                << Endl;
            allocationStats.DumpSizeStats(out, marker, sep);
            allocationStats.DumpTagStats(out, marker, sep);
            analyzer.Dump(out, stackCountLimit, sampleCountLimit, isHeap, true);
            return;
        }

        HTML(out) {
            if (isHeap) {
                out << "<p><a class=\"btn btn-primary\" href=\"?action=log\">Dump to log</a></p>\n";
            }
            TABLE_SORTABLE_CLASS("table") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { out << "upper bound"; }
                        TABLEH() { out << "size"; }
                        TABLEH() { out << "count"; }
                    }
                    out << Endl;
                }
                TABLEBODY() {
                    for (size_t b = 0; b < TAllocationStats::MaxSizeIndex; ++b) {
                        if (!allocationStats.Sizes[b]) {
                            continue;
                        }
                        TABLER() {
                            if (b == TAllocationStats::MaxSizeIndex - 1) {
                                TABLED() { out << "inf"; }
                            } else {
                                TABLED() { out << TAllocationStats::SizeUpperBound(b); }
                            }
                            TABLED() { out << allocationStats.Sizes[b]; }
                            TABLED() { out << allocationStats.Counts[b]; }
                        }
                        out << Endl;
                    }
                    TABLER() {
                        TABLED() { out << "<b>total</b>"; }
                        TABLED() { out << "<b>" << allocationStats.Size << "</b>"; }
                        TABLED() { out << "<b>" << allocationStats.Count << "</b>"; }
                    }
                    out << Endl;
                }
            }
            TABLE_SORTABLE_CLASS("table") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { out << "activity"; }
                        TABLEH() { out << "size"; }
                        TABLEH() { out << "count"; }
                    }
                    out << Endl;
                }
                TABLEBODY() {
                    for (ui32 t = 0; t < TAllocationStats::MaxTag; ++t) {
                        if (!allocationStats.TagSizes[t]) {
                            continue;
                        }
                        TABLER() {
                            TABLED() { out << TAllocationStats::GetTagName(t); }
                            TABLED() { out << allocationStats.TagSizes[t]; }
                            TABLED() { out << allocationStats.TagCounts[t]; }
                        }
                        out << Endl;
                    }
                }
            }
            PRE() {
                out << "Snapshot calculation time: " << (end - start).MicroSeconds() << " us" << Endl
                    << Endl;
                analyzer.Dump(out, stackCountLimit, sampleCountLimit, isHeap, false);
            }
        }
    }

public:
    explicit TTcMallocMonitor(TDynamicCountersPtr group) {
        CounterGroup = group->GetSubgroup("component", "tcmalloc");

        SizeHistogram = CounterGroup->GetHistogram("tcmalloc.sampled_size",
            NMonitoring::ExponentialHistogram(TAllocationStats::MaxSizeIndex, 2, 1), false);

        CountHistogram = CounterGroup->GetHistogram("tcmalloc.sampled_count",
            NMonitoring::ExponentialHistogram(TAllocationStats::MaxSizeIndex, 2, 1), false);
    }

    void RegisterPages(TMon* mon, TActorSystem* actorSystem, TActorId actorId) override {
        auto* indexPage = mon->RegisterIndexPage("memory", "Memory");
        mon->RegisterActorPage(indexPage, "heap", "Heap", false, actorSystem, actorId);
        mon->RegisterActorPage(indexPage, "peakheap", "Peak heap", false, actorSystem, actorId);
        mon->RegisterActorPage(indexPage, "fragmentation", "Fragmentation", false, actorSystem, actorId);
    }

    void RegisterControls(TIntrusivePtr<TControlBoard> icb) override {
        Controls.Register(icb);
    }

    void Update(TDuration interval) override {
        UpdateCounters();
        UpdateControls();
        ReleaseMemoryIfNecessary(interval);
    }

    void Dump(IOutputStream& out, const TString& relPath) override {
        if (relPath == "/memory/heap") {
            DumpCurrent(out, tcmalloc::ProfileType::kHeap);
            return;
        } else if (relPath == "/memory/peakheap") {
            DumpCurrent(out, tcmalloc::ProfileType::kPeakHeap);
            return;
        } else if (relPath == "/memory/fragmentation") {
            DumpCurrent(out, tcmalloc::ProfileType::kFragmentation);
            return;
        }

        auto properties = tcmalloc::MallocExtension::GetProperties();
        auto stats = tcmalloc::MallocExtension::GetStats();

        HTML(out) {
            TAG(TH3) {
                out << "TCMalloc" << Endl;
            }
            out << "<hr>" << Endl;
            TAG(TH4) {
                out << "Allocator properties" << Endl;
            }
            TABLE_CLASS("table") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { out << "Property"; }
                        TABLEH() { out << "Value"; }
                    }
                    out << Endl;
                }
                TABLEBODY() {
                    for (const auto& [name, value] : properties) {
                        TABLER() {
                            TABLED() { out << name; }
                            TABLED() { FormatPrettyNumber(out, value.value); }
                        }
                        out << Endl;
                    }
                }
            }
            out << "<hr>" << Endl;
            TAG(TH4) {
                out << "<a href=\"https://github.com/google/tcmalloc/blob/master/docs/stats.md\">Internal stats</a>" << Endl;
            }
            PRE() {
                out << EncodeHtmlPcdata(stats) << Endl;
            }
        }
    }

    void DumpForLog(IOutputStream& out, size_t limit) override {
        auto properties = tcmalloc::MallocExtension::GetProperties();
        auto stats = tcmalloc::MallocExtension::GetStats();

        const char* sep = " | ";
        out << "======== TCMALLOC PROPERTIES" << sep;
        for (const auto& [name, value] : properties) {
            out << name << ": " << value.value << sep;
        }
        out << Endl;

        out << "======== TCMALLOC STATISTICS" << Endl;
        out << stats << Endl;

        out << "======== TCMALLOC HEAP" << Endl;
        DumpCurrent(out, tcmalloc::ProfileType::kHeap, limit, 128, true);
    }
};


class TTcMallocProfiler : public IProfilerLogic {
    tcmalloc::MallocExtension::AllocationProfilingToken Token;

public:
    void Start() override {
        Token = tcmalloc::MallocExtension::StartAllocationProfiling();
    }

    void Stop(IOutputStream& out, size_t countLimit, bool forLog) override {
        auto profile = std::move(Token).Stop();

        TAllocationAnalyzer analyzer(std::move(profile));
        TAllocationStats allocationStats;
        analyzer.Prepare(&allocationStats);

        const char* marker = forLog ? "PROFILE" : "--------------------------------";
        const char* sep = forLog ? " | " : "\n";
        allocationStats.DumpSizeStats(out, marker, sep);
        allocationStats.DumpTagStats(out, marker, sep);
        analyzer.Dump(out, countLimit, 1024, false, forLog);
    }
};


std::unique_ptr<IAllocStats> CreateTcMallocStats(TDynamicCountersPtr group) {
    return std::make_unique<TTcMallocStats>(std::move(group));
}

std::unique_ptr<IAllocState> CreateTcMallocState() {
    return std::make_unique<TTcMallocState>();
}

std::unique_ptr<IAllocMonitor> CreateTcMallocMonitor(TDynamicCountersPtr group) {
    return std::make_unique<TTcMallocMonitor>(std::move(group));
}

std::unique_ptr<IProfilerLogic> CreateTcMallocProfiler() {
    return std::make_unique<TTcMallocProfiler>();
}

}
