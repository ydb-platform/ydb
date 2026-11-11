#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/scheme/scheme_tablecell.h>

#include <contrib/libs/tcmalloc/tcmalloc/malloc_extension.h>
#include <library/cpp/getopt/last_getopt.h>

#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/memory/pool.h>
#include <util/stream/file.h>
#include <util/stream/output.h>
#include <util/system/rusage.h>

#include <array>
#include <chrono>
#include <cstring>
#include <limits>
#include <optional>

namespace {
using namespace NKikimr;
using TRead = TEvDataShard::TEvRead;

struct TConfig {
    TString Mode = "legacy";
    TString Kind = "ranges";
    size_t Entries = 4096;
    size_t Requests = 8;
    size_t Cycles = 5;
    size_t Columns = 5;
    size_t ToColumns = 5;
    size_t CellBytes = 16;
    size_t ArenaChunkBytes = 64 * 1024;
    size_t BackgroundPerRequest = 32;
    size_t ProfileSamplingBytes = 64 * 1024;
    bool Profile = false;
    TString StatsFile;
};

struct TSample {
    const char* Phase;
    size_t Cycle;
    size_t Requests;
    ui64 LoadUs;
    ui64 App;
    ui64 Physical;
    ui64 Central;
    ui64 Transfer;
    ui64 Cpu;
    ui64 Thread;
    ui64 PageHeap;
    ui64 AllocatorPeak;
    ui64 Rss;
    ui64 LifetimePeakRss;
    size_t ArenaBytes = 0;
    size_t ArenaChunks = 0;
    i64 EstimatedAllocations = -1;
    i64 EstimatedAllocatedBytes = -1;
};

size_t CheckedMultiply(size_t left, size_t right) {
    Y_ENSURE(!right || left <= std::numeric_limits<size_t>::max() / right, "Benchmark size overflow");
    return left * right;
}

ui64 Property(const char* name) {
    auto value = tcmalloc::MallocExtension::GetNumericProperty(name);
    Y_ENSURE(value, "Missing TCMalloc property: " << name);
    return *value;
}

TSample Measure(const char* phase, size_t cycle = 0, size_t requests = 0, ui64 loadUs = 0) {
    return {
        phase,
        cycle,
        requests,
        loadUs,
        Property("generic.bytes_in_use_by_app"),
        Property("generic.physical_memory_used"),
        Property("tcmalloc.central_cache_free"),
        Property("tcmalloc.transfer_cache_free") + Property("tcmalloc.sharded_transfer_cache_free"),
        Property("tcmalloc.cpu_free"),
        Property("tcmalloc.thread_cache_free"),
        Property("tcmalloc.page_heap_free"),
        Property("generic.peak_memory_usage"),
        TRusage::GetCurrentRSS(),
        TRusage::Get().MaxRss,
    };
}

void AccumulateCellChecksum(ui64& checksum, TConstArrayRef<TCell> cells) {
    for (const auto& cell : cells) {
        checksum = checksum * 131 + cell.Size() + cell.IsNull();
        for (size_t i = 0; i < cell.Size(); ++i) {
            checksum = checksum * 131 + static_cast<unsigned char>(cell.Data()[i]);
        }
    }
}

TString MakeFrame(const TConfig& config, size_t columns, ui64 ordinal, ui64& checksum) {
    if (!columns) {
        return {};
    }
    TString bytes(columns * config.CellBytes, 'x');
    TVector<TCell> cells;
    cells.reserve(columns);
    char* raw = bytes.Detach();
    for (size_t col = 0; col < columns; ++col) {
        char* data = raw + col * config.CellBytes;
        for (size_t i = 0; i < config.CellBytes; ++i) {
            data[i] = 'a' + (ordinal + col + i) % 26;
        }
        cells.emplace_back(data, config.CellBytes);
    }
    AccumulateCellChecksum(checksum, cells);
    return TSerializedCellVec::Serialize(cells);
}

struct TInput {
    TIntrusivePtr<NActors::TEventSerializedData> Data;
    ui64 Checksum = 0;
};

TInput MakeInput(const TConfig& config, TVector<TSample>& samples) {
    TInput input;
    TRead producer;
    producer.Record.SetReadId(1);
    producer.Record.MutableTableId()->SetOwnerId(1);
    producer.Record.MutableTableId()->SetTableId(1);
    producer.Keys.reserve(config.Kind == "keys" ? config.Entries : 0);
    producer.Ranges.reserve(config.Kind == "ranges" ? config.Entries : 0);
    for (size_t i = 0; i < config.Entries; ++i) {
        auto from = MakeFrame(config, config.Columns, 2 * i, input.Checksum);
        if (config.Kind == "keys") {
            producer.Keys.emplace_back(std::move(from));
        } else {
            auto to = MakeFrame(config, config.ToColumns, 2 * i + 1, input.Checksum);
            producer.Ranges.emplace_back(from, to, true, false);
        }
    }
    samples.push_back(Measure("producer"));
    NActors::TAllocChunkSerializer serializer;
    Y_ENSURE(producer.SerializeToArcadiaStream(&serializer));
    auto serialized = serializer.Release(producer.CreateSerializationInfo(false));
    samples.push_back(Measure("serialized"));
    // Own an independent wire buffer after the producer and serializer are destroyed.
    input.Data =
        MakeIntrusive<NActors::TEventSerializedData>(serialized->GetString(), producer.CreateSerializationInfo(false));
    return input;
}

// Freeze the pre-change receiver algorithm so comparisons remain valid after integration.
struct TLegacyRead : NActors::TEventPB<TLegacyRead, NKikimrTxDataShard::TEvRead, TRead::EventType> {
    TVector<TSerializedCellVec> Keys;
    TVector<TSerializedTableRange> Ranges;
    bool Cancelled = false;
    NLWTrace::TOrbit Orbit;
    NWilson::TSpan ReadSpan;
};

template <typename TEvent>
void LoadTypedVectors(TEvent& event) {
    const auto& record = event.Record;
    event.Keys.reserve(record.KeysSize());
    for (const auto& key : record.GetKeys()) {
        event.Keys.emplace_back(key);
    }
    event.Ranges.reserve(record.RangesSize());
    for (const auto& range : record.GetRanges()) {
        event.Ranges.emplace_back(range);
    }
}

// Match the legacy typed vectors and auxiliary fields; vary only protobuf ownership.
struct TReadWithProtoArena
    : NActors::TEventPBWithArena<TReadWithProtoArena, NKikimrTxDataShard::TEvRead, TRead::EventType> {
    TVector<TSerializedCellVec> Keys;
    TVector<TSerializedTableRange> Ranges;
    bool Cancelled = false;
    NLWTrace::TOrbit Orbit;
    NWilson::TSpan ReadSpan;
};

class TLoadedRead {
public:
    TLoadedRead(const TConfig& config, const NActors::TEventSerializedData* data) {
        if (config.Mode == "protobuf-arena") {
            ProtoEvent.Reset(TReadWithProtoArena::Load(data));
            LoadTypedVectors(*ProtoEvent);
            return;
        }
        if (config.Mode == "legacy") {
            LegacyEvent.Reset(TLegacyRead::Load(data));
            LoadTypedVectors(*LegacyEvent);
            return;
        }
        if (config.Mode == "production") {
            Event.Reset(TRead::Load(data));
            Y_ENSURE(
                Event->PrepareReadData(config.Kind == "keys" ? config.Columns : Max(config.Columns, config.ToColumns)));
            OriginalFromColumns = config.Columns;
            OriginalToColumns = config.ToColumns;
            return;
        }
        // Retain the same protobuf backing strings in both compact prototypes.
        Event.Reset(TRead::TBase::Load(data));
        const bool keys = config.Kind == "keys";
        Endpoints.reserve(config.Entries * (keys ? 1 : 2));
        if (config.Mode == "contiguous") {
            Cells.resize_uninitialized(config.Entries * (config.Columns + (keys ? 0 : config.ToColumns)));
        } else {
            TMemoryPool::TOptions options;
            options.RoundUpToNextPowerOfTwo = false;
            Arena = MakeHolder<TMemoryPool>(
                config.ArenaChunkBytes, TMemoryPool::TLinearGrow::Instance(), TDefaultAllocator::Instance(), options);
        }
        size_t offset = 0;
        auto append = [&](const TString& buffer, size_t count) {
            TCell* ptr = count ? (Arena ? Arena->AllocateArray<TCell>(count) : Cells.data() + offset) : nullptr;
            TArrayRef<TCell> cells(ptr, count);
            Y_ENSURE(TSerializedCellVec::TryParseCellsInto(std::string_view(buffer.data(), buffer.size()), cells));
            Endpoints.emplace_back(cells);
            offset += count;
        };
        if (keys) {
            for (const auto& key : Event->Record.GetKeys()) {
                append(key, config.Columns);
            }
        } else {
            for (const auto& range : Event->Record.GetRanges()) {
                append(range.GetFrom(), config.Columns);
                append(range.GetTo(), config.ToColumns);
            }
        }
    }

    ui64 Checksum() const {
        ui64 result = 0;
        if (LegacyEvent) {
            for (const auto& key : LegacyEvent->Keys) {
                AccumulateCellChecksum(result, key.GetCells());
            }
            for (const auto& range : LegacyEvent->Ranges) {
                AccumulateCellChecksum(result, range.From.GetCells());
                AccumulateCellChecksum(result, range.To.GetCells());
            }
            return result;
        }
        if (ProtoEvent) {
            for (const auto& key : ProtoEvent->Keys) {
                AccumulateCellChecksum(result, key.GetCells());
            }
            for (const auto& range : ProtoEvent->Ranges) {
                AccumulateCellChecksum(result, range.From.GetCells());
                AccumulateCellChecksum(result, range.To.GetCells());
            }
            return result;
        }
        if (!Endpoints.empty()) {
            for (auto cells : Endpoints) {
                AccumulateCellChecksum(result, cells);
            }
        } else {
            for (size_t i = 0; i < Event->GetKeysSize(); ++i) {
                AccumulateCellChecksum(result, Event->GetKey(i).GetCells());
            }
            for (size_t i = 0; i < Event->GetRangesSize(); ++i) {
                const auto range = Event->GetRange(i);
                AccumulateCellChecksum(result, range.From.GetCells().Slice(0, OriginalFromColumns));
                AccumulateCellChecksum(result, range.To.GetCells().Slice(0, OriginalToColumns));
            }
        }
        return result;
    }

    void AddArenaStats(TSample& sample) const {
        if (Arena) {
            sample.ArenaBytes += Arena->MemoryAllocated() + Arena->MemoryWaste();
            auto count = [&](const char*, size_t) {
                ++sample.ArenaChunks;
            };
            Arena->Traverse(count);
        }
    }

private:
    THolder<TRead> Event;
    THolder<TLegacyRead> LegacyEvent;
    size_t OriginalFromColumns = 0;
    size_t OriginalToColumns = 0;
    THolder<TReadWithProtoArena> ProtoEvent;
    TVector<TCell> Cells;
    THolder<TMemoryPool> Arena;
    TVector<TConstArrayRef<TCell>> Endpoints;
};

void DumpStats(IOutputStream* out, const char* phase, size_t cycle) {
    if (out) {
        *out << "\n=== " << phase << " cycle " << cycle << " ===\n" << tcmalloc::MallocExtension::GetStats();
    }
}

void Run(const TConfig& config) {
    TVector<TSample> samples;
    samples.reserve(5 + 2 * config.Cycles);
    TVector<THolder<TLoadedRead>> reads;
    reads.reserve(config.Requests);
    TVector<TArrayHolder<char>> background;
    background.reserve(config.Cycles * config.Requests * config.BackgroundPerRequest);
    THolder<TFileOutput> stats;
    if (config.StatsFile) {
        stats = MakeHolder<TFileOutput>(config.StatsFile);
    }
    samples.push_back(Measure("before_producer"));
    auto input = MakeInput(config, samples);
    const size_t wireBytes = input.Data->GetSize();
    samples.push_back(Measure("wire_only"));
    DumpStats(stats.Get(), "wire_only", 0);
    for (size_t cycle = 0; cycle < config.Cycles; ++cycle) {
        std::optional<tcmalloc::MallocExtension::AllocationProfilingToken> token;
        if (config.Profile) {
            token.emplace(tcmalloc::MallocExtension::StartAllocationProfiling());
        }
        ui64 loadUs = 0;
        for (size_t i = 0; i < config.Requests; ++i) {
            const auto start = std::chrono::steady_clock::now();
            reads.push_back(MakeHolder<TLoadedRead>(config, input.Data.Get()));
            loadUs +=
                std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start).count();
            Y_ENSURE(reads.back()->Checksum() == input.Checksum, "Decoded cell mismatch");
            constexpr std::array<size_t, 8> sizes = { 8, 32, 64, 80, 96, 112, 128, 256 };
            for (size_t j = 0; j < config.BackgroundPerRequest; ++j) {
                TArrayHolder<char> object(new char[sizes[j % sizes.size()]]);
                std::memset(object.Get(), 'x', sizes[j % sizes.size()]);
                background.push_back(std::move(object));
            }
        }
        auto sample = Measure("loaded", cycle, reads.size(), loadUs);
        for (const auto& read : reads) {
            read->AddArenaStats(sample);
        }
        if (token) {
            auto profile = std::move(*token).Stop();
            sample.EstimatedAllocations = 0;
            sample.EstimatedAllocatedBytes = 0;
            profile.Iterate([&](const tcmalloc::Profile::Sample& allocation) {
                sample.EstimatedAllocations += allocation.count;
                sample.EstimatedAllocatedBytes += allocation.sum;
            });
        }
        samples.push_back(sample);
        DumpStats(stats.Get(), "loaded", cycle);
        reads.clear();
        samples.push_back(Measure("drained_with_background", cycle));
        DumpStats(stats.Get(), "drained_with_background", cycle);
    }
    ui64 backgroundChecksum = 0;
    for (const auto& object : background) {
        backgroundChecksum += object[0];
    }
    background.clear();
    input.Data.Reset();
    samples.push_back(Measure("all_released"));
    DumpStats(stats.Get(), "all_released", config.Cycles);
    Cerr << "Decoded checksum: " << input.Checksum << "; background checksum: " << backgroundChecksum << Endl;
    Cout << "mode,kind,entries,columns,to_columns,cell_bytes,wire_bytes,profiled,stats_enabled,background_per_request,"
            "arena_chunk_hint,profile_sampling_bytes,phase,cycle,active_requests,load_us,"
            "app_bytes,physical_bytes,central_free_bytes,transfer_free_bytes,cpu_free_bytes,thread_free_bytes,"
            "page_heap_free_bytes,raw_tcmalloc_peak_memory_usage_bytes,rss_bytes,lifetime_peak_rss_bytes,"
            "arena_requested_chunk_bytes,arena_chunks,estimated_allocations,estimated_allocated_bytes\n";
    for (const auto& sample : samples) {
        Cout << config.Mode << ',' << config.Kind << ',' << config.Entries << ',' << config.Columns << ','
             << config.ToColumns << ',' << config.CellBytes << ',' << wireBytes << ',' << config.Profile << ','
             << bool(config.StatsFile) << ',' << config.BackgroundPerRequest << ',' << config.ArenaChunkBytes << ','
             << (config.Profile ? config.ProfileSamplingBytes : 0) << ',' << sample.Phase << ',' << sample.Cycle << ','
             << sample.Requests << ',' << sample.LoadUs << ',' << sample.App << ',' << sample.Physical << ','
             << sample.Central << ',' << sample.Transfer << ',' << sample.Cpu << ',' << sample.Thread << ','
             << sample.PageHeap << ',' << sample.AllocatorPeak << ',' << sample.Rss << ',' << sample.LifetimePeakRss
             << ',' << sample.ArenaBytes << ',' << sample.ArenaChunks << ',' << sample.EstimatedAllocations << ','
             << sample.EstimatedAllocatedBytes << '\n';
    }
}
} // namespace

int main(int argc, const char** argv) {
    try {
        TConfig config;
        auto opts = NLastGetopt::TOpts::Default();
        opts.SetFreeArgsNum(0);
        opts.AddLongOption("mode", "legacy, contiguous, arena, protobuf-arena or production").StoreResult(&config.Mode);
        opts.AddLongOption("kind", "keys or ranges").StoreResult(&config.Kind);
        opts.AddLongOption("entries", "Entries per request").StoreResult(&config.Entries);
        opts.AddLongOption("requests", "Simultaneously retained reads").StoreResult(&config.Requests);
        opts.AddLongOption("cycles", "Load/drain cycles in this process").StoreResult(&config.Cycles);
        opts.AddLongOption("columns", "Cells per key or From endpoint").StoreResult(&config.Columns);
        opts.AddLongOption("to-columns", "Cells per To endpoint; zero means absent").StoreResult(&config.ToColumns);
        opts.AddLongOption("cell-bytes", "Bytes in each cell").StoreResult(&config.CellBytes);
        opts.AddLongOption("arena-chunk-bytes", "Arena data hint, excluding pool header/allocator rounding")
            .StoreResult(&config.ArenaChunkBytes);
        opts.AddLongOption("background-per-request", "Long-lived small objects interleaved between reads")
            .StoreResult(&config.BackgroundPerRequest);
        opts.AddLongOption("profile", "Enable sampled allocation counts; perturbs memory measurements")
            .NoArgument()
            .SetFlag(&config.Profile);
        opts.AddLongOption("profile-sampling-bytes", "Sampling interval for separate profiling runs")
            .StoreResult(&config.ProfileSamplingBytes);
        opts.AddLongOption("stats-file", "Optional full allocator stats; diagnostic run, perturbs measurements")
            .StoreResult(&config.StatsFile);
        NLastGetopt::TOptsParseResult parsed(&opts, argc, argv);
        Y_ENSURE(config.Mode == "legacy" || config.Mode == "contiguous" || config.Mode == "arena" ||
                     config.Mode == "protobuf-arena" || config.Mode == "production", "Unknown mode");
        Y_ENSURE(config.Kind == "keys" || config.Kind == "ranges", "Unknown kind");
        Y_ENSURE(config.Entries && config.Requests && config.Cycles, "entries, requests and cycles must be positive");
        Y_ENSURE(config.Columns <= std::numeric_limits<ui16>::max(), "Invalid From/key column count");
        Y_ENSURE(config.ToColumns <= std::numeric_limits<ui16>::max(), "Invalid To column count");
        Y_ENSURE(config.CellBytes && config.CellBytes < (ui64(1) << 31), "Invalid cell size");
        Y_ENSURE(config.ArenaChunkBytes && config.ArenaChunkBytes <= std::numeric_limits<size_t>::max() / 2,
            "arena-chunk-bytes must leave room for pool headers and alignment");
        Y_ENSURE(config.Entries <= static_cast<size_t>(std::numeric_limits<int>::max()), "Too many protobuf entries");
        const size_t endpointColumns = config.Columns + (config.Kind == "keys" ? 0 : config.ToColumns);
        const size_t cellCount = CheckedMultiply(config.Entries, endpointColumns);
        CheckedMultiply(cellCount, sizeof(TCell));
        CheckedMultiply(config.Columns, config.CellBytes);
        CheckedMultiply(config.ToColumns, config.CellBytes);
        const size_t frameBytes = CheckedMultiply(endpointColumns, config.CellBytes + sizeof(ui32));
        const size_t maxWireBytes = std::numeric_limits<ui32>::max();
        Y_ENSURE(frameBytes <= maxWireBytes - 64 && config.Entries <= (maxWireBytes - 128) / (frameBytes + 64),
            "Encoded request exceeds the event size representation");
        CheckedMultiply(CheckedMultiply(config.Cycles, config.Requests), config.BackgroundPerRequest);
        Y_ENSURE(config.Cycles <= (std::numeric_limits<size_t>::max() - 5) / 2, "Too many cycles");
        if (config.Profile) {
            Y_ENSURE(config.ProfileSamplingBytes, "profile-sampling-bytes must be positive");
            tcmalloc::MallocExtension::SetProfileSamplingInterval(config.ProfileSamplingBytes);
        }
        Run(config);
        return 0;
    } catch (const std::exception& e) {
        Cerr << e.what() << Endl;
        return 1;
    }
}
