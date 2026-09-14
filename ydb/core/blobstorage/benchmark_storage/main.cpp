// A process-per-cell, real-clock benchmark. All PDisks are owned temporary files.
#include <ydb/core/blobstorage/ut_vdisk/lib/prepare.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy_nodemon.h>
#include <ydb/core/blobstorage/storagepoolmon/storagepool_counters.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_data.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_partlayout.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h>
#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress.h>
#include <ydb/core/blobstorage/vdisk/huge/blobstorage_hullhugeheap.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_blob.h>
#include <ydb/core/blobstorage/vdisk/skeleton/blobstorage_skeletonfront.h>
#include <ydb/core/blobstorage/vdisk/syncer/blobstorage_syncer_localwriter.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogmsgwriter.h>
#include <ydb/core/load_test/service_actor.h>
#include <ydb/core/erasure/benchmark/isa_dispatch.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/mon_stats.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/protobuf/util/pb_io.h>
#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/string/cast.h>
#include <util/system/hp_timer.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <future>
#include <map>
#include <mutex>
#include <thread>
#include <time.h>
#include <linux/perf_event.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>

using namespace NActors;
using namespace NKikimr;

namespace {
ui64 ClockNs(clockid_t clock = CLOCK_MONOTONIC) {
    timespec value;
    Y_ABORT_UNLESS(clock_gettime(clock, &value) == 0);
    return ui64(value.tv_sec) * 1000000000 + value.tv_nsec;
}

// Matches ContentType=Validated in load_test/group_write.cpp: the native
// TLogoBlobID bytes repeat across the whole logical payload, including its tail.
bool ValidateLoadPayload(const TLogoBlobID& id, const TRope& data) {
    if (data.size() != id.BlobSize()) return false;
    std::array<char, sizeof(TLogoBlobID) * 256> pattern;
    memcpy(pattern.data(), &id, sizeof(id));
    for (size_t filled = sizeof(id); filled < pattern.size(); filled *= 2) {
        memcpy(pattern.data() + filled, pattern.data(), filled);
    }
    size_t offset = 0;
    for (auto it = data.Begin(); offset < data.size();) {
        const size_t size = it.ContiguousSize();
        const char* bytes = it.ContiguousData();
        for (size_t pos = 0; pos < size;) {
            const size_t start = (offset + pos) % sizeof(id);
            const size_t length = Min(size - pos, pattern.size() - start);
            if (memcmp(bytes + pos, pattern.data() + start, length)) return false;
            pos += length;
        }
        offset += size;
        it += size;
    }
    return true;
}

void VerifyLoadPayloadValidator() {
    const TLogoBlobID id(8202002, 1, 3, 0, sizeof(TLogoBlobID) * 512 + 29, 5);
    TString bytes = TString::Uninitialized(id.BlobSize());
    char* buffer = bytes.Detach();
    const char* reference = reinterpret_cast<const char*>(&id);
    for (size_t i = 0; i < bytes.size(); ++i) buffer[i] = reference[i % sizeof(id)];
    auto fragmented = [](const TString& bytes) {
        TRope result(TString(bytes.substr(0, 7)));
        result.Insert(result.End(), TRope(TString(bytes.substr(7, 37))));
        result.Insert(result.End(), TRope(TString(bytes.substr(44))));
        return result;
    };
    Y_ENSURE(ValidateLoadPayload(id, fragmented(bytes)), "valid fragmented LoadActor payload rejected");
    for (const size_t offset : {size_t(0), size_t(7), sizeof(TLogoBlobID) * 256 - 1,
            sizeof(TLogoBlobID) * 256, bytes.size() - 1}) {
        TString damaged = bytes;
        damaged.Detach()[offset] ^= 1;
        Y_ENSURE(!ValidateLoadPayload(id, fragmented(damaged)), "corrupted LoadActor payload accepted");
    }
    bytes.resize(bytes.size() - 1);
    Y_ENSURE(!ValidateLoadPayload(id, TRope(bytes)), "truncated LoadActor payload accepted");
}

class TProcessPmu {
    enum class EScope { User, FullTask };
    struct TDescriptor { const char* Name; ui32 Type; ui64 Config; EScope Scope; };
    struct TCounter { TDescriptor Descriptor; int Fd; TString Error; };
    TVector<TCounter> Counters;

    static TString SyscallError(const char* operation) {
        const int error = errno;
        return TStringBuilder() << operation << ": " << strerror(error) << " (errno " << error << ")";
    }
public:
    TProcessPmu() {
        constexpr auto cacheRead = [](ui64 cache, ui64 result) {
            return cache | (ui64(PERF_COUNT_HW_CACHE_OP_READ) << 8) | (result << 16);
        };
        constexpr TDescriptor descriptors[] = {
            {"cycles", PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES, EScope::User},
            {"instructions", PERF_TYPE_HARDWARE, PERF_COUNT_HW_INSTRUCTIONS, EScope::User},
            {"ref_cycles", PERF_TYPE_HARDWARE, PERF_COUNT_HW_REF_CPU_CYCLES, EScope::User},
            {"cache_references", PERF_TYPE_HARDWARE, PERF_COUNT_HW_CACHE_REFERENCES, EScope::User},
            {"cache_misses", PERF_TYPE_HARDWARE, PERF_COUNT_HW_CACHE_MISSES, EScope::User},
            {"branches", PERF_TYPE_HARDWARE, PERF_COUNT_HW_BRANCH_INSTRUCTIONS, EScope::User},
            {"branch_misses", PERF_TYPE_HARDWARE, PERF_COUNT_HW_BRANCH_MISSES, EScope::User},
            {"dtlb_read_accesses", PERF_TYPE_HW_CACHE,
                cacheRead(PERF_COUNT_HW_CACHE_DTLB, PERF_COUNT_HW_CACHE_RESULT_ACCESS), EScope::User},
            {"dtlb_read_misses", PERF_TYPE_HW_CACHE,
                cacheRead(PERF_COUNT_HW_CACHE_DTLB, PERF_COUNT_HW_CACHE_RESULT_MISS), EScope::User},
            {"itlb_read_misses", PERF_TYPE_HW_CACHE,
                cacheRead(PERF_COUNT_HW_CACHE_ITLB, PERF_COUNT_HW_CACHE_RESULT_MISS), EScope::User},
            {"task_clock", PERF_TYPE_SOFTWARE, PERF_COUNT_SW_TASK_CLOCK, EScope::FullTask},
            {"context_switches", PERF_TYPE_SOFTWARE, PERF_COUNT_SW_CONTEXT_SWITCHES, EScope::FullTask},
            {"cpu_migrations", PERF_TYPE_SOFTWARE, PERF_COUNT_SW_CPU_MIGRATIONS, EScope::FullTask},
        };
        for (const auto& descriptor : descriptors) {
            perf_event_attr attr{};
            attr.type = descriptor.Type;
            attr.size = sizeof(attr);
            attr.config = descriptor.Config;
            attr.disabled = 1;
            attr.inherit = 1;
            attr.exclude_kernel = descriptor.Scope == EScope::User;
            attr.exclude_hv = descriptor.Scope == EScope::User;
            attr.read_format = PERF_FORMAT_TOTAL_TIME_ENABLED | PERF_FORMAT_TOTAL_TIME_RUNNING;
            const int fd = syscall(SYS_perf_event_open, &attr, 0, -1, -1, PERF_FLAG_FD_CLOEXEC);
            Counters.push_back({descriptor, fd, fd < 0 ? SyscallError("perf_event_open") : TString()});
        }
    }
    ~TProcessPmu() { for (const auto& item : Counters) if (item.Fd >= 0) close(item.Fd); }
    void Begin() {
        for (auto& item : Counters) {
            if (item.Fd >= 0) {
                if (ioctl(item.Fd, PERF_EVENT_IOC_RESET, 0)) item.Error = SyscallError("PERF_EVENT_IOC_RESET");
                else if (ioctl(item.Fd, PERF_EVENT_IOC_ENABLE, 0)) item.Error = SyscallError("PERF_EVENT_IOC_ENABLE");
            }
        }
    }
    NJson::TJsonValue End() {
        NJson::TJsonValue result(NJson::JSON_MAP);
        result["scope"] = "perf_event_open on main thread before actor startup, inherit=1; all subsequently created worker threads; privilege scope is recorded per counter";
        for (auto& item : Counters) {
            const auto& descriptor = item.Descriptor;
            auto& counter = result[descriptor.Name];
            counter["type"] = descriptor.Type;
            counter["config"] = descriptor.Config;
            counter["scope"] = descriptor.Scope == EScope::User ? "user CPU only" : "full task, including kernel";
            counter["unit"] = descriptor.Config == PERF_COUNT_SW_TASK_CLOCK && descriptor.Type == PERF_TYPE_SOFTWARE
                ? "nanoseconds" : "events";
            if (item.Fd >= 0) {
                if (ioctl(item.Fd, PERF_EVENT_IOC_DISABLE, 0) && item.Error.empty()) {
                    item.Error = SyscallError("PERF_EVENT_IOC_DISABLE");
                }
                ui64 values[3]{};
                const ssize_t bytes = read(item.Fd, values, sizeof(values));
                if (bytes == static_cast<ssize_t>(sizeof(values))) {
                    counter["raw_count"] = values[0];
                    counter["time_enabled_ns"] = values[1];
                    counter["time_running_ns"] = values[2];
                    if (item.Error.empty() && values[2]) {
                        counter["status"] = "available";
                        counter["scaled_count"] = double(values[0]) * values[1] / values[2];
                    } else if (item.Error.empty()) {
                        item.Error = "event was not scheduled (time_running_ns is zero)";
                    }
                } else {
                    const TString error = bytes < 0 ? SyscallError("read") :
                        TString(TStringBuilder() << "short read: " << bytes << " bytes, expected " << sizeof(values));
                    if (!item.Error.empty()) item.Error += "; ";
                    item.Error += error;
                }
            }
            if (!item.Error.empty()) {
                counter["status"] = "unavailable";
                counter["reason"] = item.Error;
            }
        }
        return result;
    }
};

struct TOptions {
    TString Species = "block-8-2";
    TString Workload = "get";
    TString Output;
    ui32 Size = 1 << 20;
    ui32 Inflight = 4;
    ui32 Seconds = 8;
    ui32 Warmup = 2;
    ui32 Corpus = 128;
    ui32 Missing = 0;
    ui32 Crc = TErasureType::CrcModeNone;
    ui64 DiskSize = 4ull << 30;
    ui64 Tablet = 8202001;

    TOptions(int argc, char** argv) {
        for (int i = 1; i < argc; i += 2) {
            Y_ENSURE(i + 1 < argc, "each option needs a value");
            const TString name(argv[i]), value(argv[i + 1]);
            if (name == "--species") Species = value;
            else if (name == "--workload") Workload = value;
            else if (name == "--output") Output = value;
            else if (name == "--size") Size = FromString<ui32>(value);
            else if (name == "--inflight") Inflight = FromString<ui32>(value);
            else if (name == "--seconds") Seconds = FromString<ui32>(value);
            else if (name == "--warmup") Warmup = FromString<ui32>(value);
            else if (name == "--corpus") Corpus = FromString<ui32>(value);
            else if (name == "--missing") Missing = FromString<ui32>(value);
            else if (name == "--crc") {
                Y_ENSURE(value == "none" || value == "whole");
                Crc = value == "whole" ? TErasureType::CrcModeWholePart : TErasureType::CrcModeNone;
            }
            else if (name == "--disk-size") DiskSize = FromString<ui64>(value);
            else ythrow yexception() << "unknown option " << name;
        }
        Y_ENSURE(Species == "block-4-2" || Species == "block-8-2");
        Y_ENSURE(Workload == "put" || Workload == "get" || Workload == "restore" || Workload == "replication" || Workload == "scrub");
        Y_ENSURE(!Output.empty() && Size && Inflight && Seconds && Corpus && Missing <= 2);
        Y_ENSURE(Size <= (10u << 20));
        Y_ENSURE((Workload != "restore" && Workload != "replication" && Workload != "scrub") || Missing);
    }
};

struct TDiskMetrics {
    TActorId Skeleton; // protected by TState::Mutex; captured during recovery
    std::atomic<ui64> VPut{0}, VMultiPut{0}, VGet{0}, VWriteBytes{0}, VReadBytes{0};
    std::atomic<ui64> ChunkRead{0}, ChunkWrite{0}, ChunkReadBytes{0}, ChunkWriteBytes{0}, LogBytes{0};
    std::atomic<ui64> InjectedReadErrors{0}, ScrubSuccess{0};
};

struct TState {
    std::atomic<bool> Measuring{false};
    std::atomic<ui64> FirstRequestNs{0}, BeginNs{0};
    std::atomic<ui64> LastResponseNs{0}, ActorFirstUs{0}, ActorLastUs{0};
    std::atomic<ui64> Requests{0}, LogicalBytes{0}, Errors{0};
    std::atomic<ui64> ValidationErrors{0}, TailStatusErrors{0}, LoadFinishedNs{0}, LoadStopRequestedNs{0};
    bool MeasurementFinished = false;
    ui64 Outstanding = 0, BeganBeforeWindow = 0;
    ui64 RepairVerificationRequests = 0, RepairVerificationBytes = 0;
    ui64 ObservedRecoveredPartBytes = 0;
    std::mutex Mutex;
    TVector<ui64> LatencyNs;
    TVector<std::unique_ptr<TDiskMetrics>> Disks;
    struct TFault { ui32 Disk; ui32 Chunk; ui32 Begin; ui32 End; };
    TVector<TFault> Faults;
    std::atomic<bool> FaultsEnabled{false}, ScrubEnabled{false};
    explicit TState(ui32 count) {
        for (ui32 i = 0; i < count; ++i) Disks.emplace_back(new TDiskMetrics);
    }
    void ResetMetrics() {
        Requests = LogicalBytes = Errors = 0;
        FirstRequestNs = 0;
        LastResponseNs = ActorFirstUs = ActorLastUs = 0;
        LatencyNs.clear();
        for (auto& disk : Disks) {
            disk->VPut = disk->VMultiPut = disk->VGet = disk->VWriteBytes = disk->VReadBytes = 0;
            disk->ChunkRead = disk->ChunkWrite = disk->ChunkReadBytes = disk->ChunkWriteBytes = disk->LogBytes = 0;
            disk->InjectedReadErrors = disk->ScrubSuccess = 0;
        }
    }
};

class TFailureDiagnostics {
    const TString Output;
    TState& State;
    NJson::TJsonValue& Result;
    bool Complete = false;
public:
    TFailureDiagnostics(const TString& output, TState& state, NJson::TJsonValue& result)
        : Output(output), State(state), Result(result) {}
    void Success() { Complete = true; }
    ~TFailureDiagnostics() noexcept {
        if (Complete) return;
        try {
            Result["status"] = "failed";
            Result["diagnostics_incomplete"] = true;
            Result["validation_errors_total"] = State.ValidationErrors.load();
            Result["unmeasured_tail_status_errors"] = State.TailStatusErrors.load();
            Result["load_finished_monotonic_ns"] = State.LoadFinishedNs.load();
            Result["load_stop_requested_monotonic_ns"] = State.LoadStopRequestedNs.load();
            auto& snapshot = Result["failure_snapshot"];
            snapshot["requests"] = State.Requests.load();
            snapshot["request_errors"] = State.Errors.load();
            snapshot["logical_bytes"] = State.LogicalBytes.load();
            {
                std::lock_guard lock(State.Mutex);
                snapshot["outstanding_requests"] = State.Outstanding;
                for (const ui64 latency : State.LatencyNs) snapshot["raw_latency_ns"].AppendValue(latency);
            }
            for (ui32 i = 0; i < State.Disks.size(); ++i) {
                const auto& metrics = *State.Disks[i];
                NJson::TJsonValue disk(NJson::JSON_MAP);
                disk["order"] = i;
                disk["vdisk_write_payload_bytes"] = metrics.VWriteBytes.load();
                disk["vdisk_read_payload_bytes"] = metrics.VReadBytes.load();
                disk["pdisk_chunk_write_bytes"] = metrics.ChunkWriteBytes.load();
                disk["pdisk_chunk_read_bytes"] = metrics.ChunkReadBytes.load();
                disk["pdisk_log_payload_bytes"] = metrics.LogBytes.load();
                snapshot["disks"].AppendValue(std::move(disk));
            }
            TFileOutput output(Output + "/partial-result.json");
            NJson::WriteJson(&output, &Result, true);
        } catch (const std::exception& error) {
            Cerr << "Could not save partial benchmark diagnostics: " << error.what() << Endl;
        }
    }
};

class TDiskObserver final : public TDecorator {
    TState& State;
    TDiskMetrics& M;
    ui32 Index;
    bool PDisk;
public:
    TDiskObserver(THolder<IActor> actor, TState& state, ui32 index, bool pdisk)
        : TDecorator(std::move(actor)), State(state), M(*state.Disks[index]), Index(index), PDisk(pdisk) {}

    bool DoBeforeReceiving(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) override {
        if (!PDisk && ev->GetTypeRewrite() == TEvFrontRecoveryStatus::EventType) {
            std::lock_guard lock(State.Mutex);
            M.Skeleton = ev->Sender;
        }
        if (PDisk && State.FaultsEnabled.load(std::memory_order_acquire) &&
                ev->GetTypeRewrite() == NPDisk::TEvChunkRead::EventType) {
            const auto* request = ev->Get<NPDisk::TEvChunkRead>();
            for (const auto& fault : State.Faults) {
                if (fault.Disk == Index && fault.Chunk == request->ChunkIdx &&
                        fault.Begin < request->Offset + request->Size && request->Offset < fault.End) {
                    auto* result = new NPDisk::TEvChunkReadResult(NKikimrProto::CORRUPTED, request->ChunkIdx,
                        request->Offset, request->Cookie, 0, "benchmark identified extent read error");
                    result->Data.AddGap(request->Offset, request->Offset + request->Size);
                    result->Data.SetData(TRcBuf::Uninitialized(request->Size));
                    ++M.InjectedReadErrors;
                    ctx.Send(ev->Sender, result, 0, ev->Cookie);
                    return false;
                }
            }
        }
        if (!State.Measuring.load(std::memory_order_relaxed)) return true;
        const ui32 type = ev->GetTypeRewrite();
        if (PDisk) {
            if (type == NPDisk::TEvChunkRead::EventType) {
                ++M.ChunkRead;
                M.ChunkReadBytes += ev->Get<NPDisk::TEvChunkRead>()->Size;
            } else if (type == NPDisk::TEvChunkWrite::EventType) {
                ++M.ChunkWrite;
                const auto* request = ev->Get<NPDisk::TEvChunkWrite>();
                M.ChunkWriteBytes += request->PartsPtr ? request->PartsPtr->ByteSize() : 0;
            } else if (type == NPDisk::TEvLog::EventType) {
                M.LogBytes += ev->Get<NPDisk::TEvLog>()->Data.size();
            }
        } else if (type == TEvBlobStorage::TEvVPut::EventType) {
            ++M.VPut;
            M.VWriteBytes += ev->Get<TEvBlobStorage::TEvVPut>()->GetBufferBytes();
        } else if (type == TEvBlobStorage::TEvVMultiPut::EventType) {
            ++M.VMultiPut;
            M.VWriteBytes += ev->Get<TEvBlobStorage::TEvVMultiPut>()->GetBufferBytes();
        } else if (type == TEvBlobStorage::TEvVGet::EventType) {
            ++M.VGet;
        } else if (type == TEvVDiskRequestCompleted::EventType) {
            auto& result = ev->Get<TEvVDiskRequestCompleted>()->Event;
            if (result && result->GetTypeRewrite() == TEvBlobStorage::TEvVGetResult::EventType) {
                auto* get = result->Get<TEvBlobStorage::TEvVGetResult>();
                for (const auto& item : get->Record.GetResult()) {
                    if (item.GetStatus() == NKikimrProto::OK) M.VReadBytes += get->GetBlobData(item).size();
                }
            }
        }
        return true;
    }
};

// A local scrub admission service only. VDisk scrub and repair actors are the
// production implementations; this does not measure BSC scheduling policies.
class TScrubScheduler final : public TActorBootstrapped<TScrubScheduler> {
    TState& State;
    struct TSlot {
        TActorId Sender;
        ui64 Cookie = 0;
        ui32 Node = 0, PDisk = 0, Slot = 0;
        bool Pending = false;
        std::optional<TString> Progress;
    };
    std::map<ui32, TSlot> Slots;
public:
    explicit TScrubScheduler(TState& state) : State(state) {}
    void Bootstrap() {
        Become(&TThis::StateFunc);
        Schedule(TDuration::MilliSeconds(100), new TEvents::TEvWakeup);
    }
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            case TEvBlobStorage::TEvControllerScrubQueryStartQuantum::EventType: {
                const auto& id = ev->Get<TEvBlobStorage::TEvControllerScrubQueryStartQuantum>()->Record.GetVSlotId();
                auto& slot = Slots[id.GetPDiskId()];
                slot.Sender = ev->Sender;
                slot.Cookie = ev->Cookie;
                slot.Node = id.GetNodeId();
                slot.PDisk = id.GetPDiskId();
                slot.Slot = id.GetVSlotId();
                slot.Pending = true;
                break;
            }
            case TEvBlobStorage::TEvControllerScrubQuantumFinished::EventType: {
                const auto& record = ev->Get<TEvBlobStorage::TEvControllerScrubQuantumFinished>()->Record;
                auto& slot = Slots[record.GetVSlotId().GetPDiskId()];
                slot.Progress = record.HasState() ? std::make_optional(record.GetState()) : std::nullopt;
                if (record.HasSuccess() && record.GetSuccess()) ++State.Disks.at(record.GetVSlotId().GetPDiskId() - 1)->ScrubSuccess;
                break;
            }
            case TEvents::TEvWakeup::EventType:
                if (State.ScrubEnabled) {
                    for (auto& [id, slot] : Slots) {
                        Y_UNUSED(id);
                        if (slot.Pending) {
                            Send(slot.Sender, new TEvBlobStorage::TEvControllerScrubStartQuantum(slot.Node, slot.PDisk,
                                slot.Slot, slot.Progress), 0, slot.Cookie);
                            slot.Pending = false;
                        }
                    }
                }
                Schedule(TDuration::MilliSeconds(100), new TEvents::TEvWakeup);
                break;
        }
    }
};

// Keeps production DSProxy and VDisk queues intact. Only the group API sender is
// relayed, to collect actual end-to-end completion latency outside LoadActor HTML.
class TGroupObserver final : public TActorBootstrapped<TGroupObserver> {
    struct TPending { TActorId Sender; ui64 Cookie; ui64 Start; ui64 Bytes; TLogoBlobID Id; bool Matches; };
    TActorId Proxy;
    TState& State;
    bool Read;
    bool ValidatedLoad;
    ui64 Tablet;
    ui32 Size;
    ui64 Cookie = 0;
    std::map<ui64, TPending> Pending;
public:
    TGroupObserver(TActorId proxy, TState& state, bool read, bool validatedLoad, ui64 tablet, ui32 size)
        : Proxy(proxy), State(state), Read(read), ValidatedLoad(validatedLoad), Tablet(tablet), Size(size) {}
    void Bootstrap() { Become(&TThis::StateFunc); }
    STFUNC(StateFunc) {
        const auto type = ev->GetTypeRewrite();
        if (type == TEvBlobStorage::TEvPut::EventType || type == TEvBlobStorage::TEvGet::EventType) {
            const bool read = type == TEvBlobStorage::TEvGet::EventType;
            const ui64 now = ClockNs();
            TLogoBlobID id;
            bool matches;
            if (read) {
                const auto* get = ev->Get<TEvBlobStorage::TEvGet>();
                id = get->Queries[0].Id;
                matches = get->QuerySize == 1 && get->Queries[0].Shift == 0 &&
                    (!get->Queries[0].Size || get->Queries[0].Size == Size);
            } else {
                id = ev->Get<TEvBlobStorage::TEvPut>()->Id;
                matches = ev->Get<TEvBlobStorage::TEvPut>()->Buffer.size() == Size;
            }
            matches &= id.TabletID() == Tablet && id.BlobSize() == Size && id.Step() != Max<ui32>() && !id.PartId();
            if (read == Read && matches) {
                std::lock_guard lock(State.Mutex);
                ++State.Outstanding;
                ui64 zero = 0;
                if (State.FirstRequestNs.compare_exchange_strong(zero, now)) {
                    State.ActorFirstUs = TActivationContext::Monotonic().MicroSeconds();
                }
            }
            const ui64 bytes = read ? 0 : ev->Get<TEvBlobStorage::TEvPut>()->Buffer.size();
            const ui64 cookie = ++Cookie;
            Pending.emplace(cookie, TPending{ev->Sender, ev->Cookie, now, bytes, id, matches});
            Send(Proxy, ev->ReleaseBase().Release(), 0, cookie);
        } else if (type == TEvBlobStorage::TEvPutResult::EventType || type == TEvBlobStorage::TEvGetResult::EventType) {
            const auto it = Pending.find(ev->Cookie);
            Y_ABORT_UNLESS(it != Pending.end());
            const auto pending = it->second;
            Pending.erase(it);
            const bool read = type == TEvBlobStorage::TEvGetResult::EventType;
            ui64 bytes = pending.Bytes;
            bool ok = true;
            bool nonOkStatus = false, shapeValid = true, payloadValid = true;
            if (read) {
                const auto* result = ev->Get<TEvBlobStorage::TEvGetResult>();
                ok = result->Status == NKikimrProto::OK;
                nonOkStatus = !ok;
                if (pending.Matches && ok) shapeValid = result->ResponseSz == 1;
                for (ui32 i = 0; i < result->ResponseSz; ++i) {
                    const bool itemOk = result->Responses[i].Status == NKikimrProto::OK;
                    nonOkStatus |= !itemOk;
                    ok &= itemOk;
                    bytes += result->Responses[i].Buffer.size();
                    if (pending.Matches && itemOk) {
                        payloadValid &= result->Responses[i].Id == pending.Id && result->Responses[i].Buffer.size() == Size;
                        if (ValidatedLoad) payloadValid &= ValidateLoadPayload(pending.Id, result->Responses[i].Buffer);
                    }
                }
                ok &= shapeValid && payloadValid;
            } else {
                ok = ev->Get<TEvBlobStorage::TEvPutResult>()->Status == NKikimrProto::OK;
                nonOkStatus = !ok;
            }
            if (pending.Matches) {
                std::lock_guard lock(State.Mutex);
                if (!ok) {
                    // LoadActor sends its final hard GC before draining requests.
                    // Non-OK tail statuses are censored; malformed/successfully
                    // returned corrupt payloads always invalidate the entire run.
                    if (ValidatedLoad && State.MeasurementFinished && nonOkStatus && shapeValid && payloadValid) {
                        ++State.TailStatusErrors;
                    } else {
                        ++State.ValidationErrors;
                    }
                }
                if (read == Read) {
                    --State.Outstanding;
                    State.LastResponseNs = ClockNs();
                    State.ActorLastUs = TActivationContext::Monotonic().MicroSeconds();
                    if (State.Measuring) {
                        ++State.Requests;
                        State.LogicalBytes += bytes;
                        State.BeganBeforeWindow += pending.Start < State.BeginNs;
                        if (!ok) ++State.Errors;
                        State.LatencyNs.push_back(ClockNs() - pending.Start);
                    }
                }
            }
            Send(pending.Sender, ev->ReleaseBase().Release(), 0, pending.Cookie);
        } else {
            Send(ev->Forward(Proxy));
        }
    }
};

using TResultPromise = std::promise<std::unique_ptr<IEventHandle>>;
class TRequest final : public TActorBootstrapped<TRequest> {
    TActorId Target;
    std::unique_ptr<IEventBase> Request;
    ui32 ResultType;
    std::shared_ptr<TResultPromise> Promise;
public:
    TRequest(TActorId target, IEventBase* request, ui32 resultType, std::shared_ptr<TResultPromise> promise)
        : Target(target), Request(request), ResultType(resultType), Promise(std::move(promise)) {}
    void Bootstrap() {
        Become(&TThis::StateFunc);
        Send(Target, Request.release());
        Schedule(TDuration::Seconds(120), new TEvents::TEvWakeup);
    }
    STFUNC(StateFunc) {
        if (ev->GetTypeRewrite() == ResultType) {
            Promise->set_value(std::unique_ptr<IEventHandle>(ev.Release()));
            PassAway();
        } else if (ev->GetTypeRewrite() == TEvents::TEvWakeup::EventType) {
            Promise->set_exception(std::make_exception_ptr(std::runtime_error("actor request timed out")));
            PassAway();
        }
    }
};

template<class TResponse, class TMessage>
std::unique_ptr<IEventHandle> Query(TConfiguration& cfg, TActorId target, TMessage* request) {
    auto promise = std::make_shared<TResultPromise>();
    auto future = promise->get_future();
    cfg.ActorSystem1->Register(new TRequest(target, request, TResponse::EventType, promise));
    Y_ENSURE(future.wait_for(std::chrono::seconds(125)) == std::future_status::ready, "request timeout");
    return future.get();
}

class TLoadParent final : public TActorBootstrapped<TLoadParent> {
    TEvLoadTestRequest::TStorageLoad Config;
    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
    std::shared_ptr<TResultPromise> Promise;
    TState& State;
    TActorId Child;
public:
    TLoadParent(TEvLoadTestRequest::TStorageLoad config, TIntrusivePtr<NMonitoring::TDynamicCounters> counters,
            std::shared_ptr<TResultPromise> promise, TState& state)
        : Config(std::move(config)), Counters(std::move(counters)), Promise(std::move(promise)), State(state) {}
    void Bootstrap() {
        Become(&TThis::StateFunc);
        Child = Register(CreateWriterLoadTest(Config, SelfId(), Counters, 1));
    }
    STFUNC(StateFunc) {
        if (ev->GetTypeRewrite() == TEvents::TEvPoisonPill::EventType) {
            State.LoadStopRequestedNs = ClockNs();
            Send(Child, new TEvents::TEvPoisonPill);
        } else if (ev->GetTypeRewrite() == TEvLoad::TEvLoadTestFinished::EventType) {
            State.LoadFinishedNs = ClockNs();
            Promise->set_value(std::unique_ptr<IEventHandle>(ev.Release()));
            PassAway();
        }
    }
};

TLogoBlobID Blob(const TOptions& opts, ui32 index) {
    return TLogoBlobID(opts.Tablet, 1, index + 1, 0, opts.Size, 0, 0, opts.Crc);
}

class TDriver final : public TActorBootstrapped<TDriver> {
    const TOptions Options;
    const TString Data;
    std::shared_ptr<std::promise<void>> Promise;
    ui64 Deadline = 0;
    ui32 Next = 0, Outstanding = 0;
    bool Once;
    bool Put;
public:
    TDriver(TOptions opts, TString data, std::shared_ptr<std::promise<void>> promise, bool once, bool put)
        : Options(std::move(opts)), Data(std::move(data)), Promise(std::move(promise)), Once(once), Put(put) {}
    void Bootstrap() {
        Become(&TThis::StateFunc);
        Deadline = ClockNs() + ui64(Options.Seconds) * 1000000000;
        Issue();
    }
    void Issue() {
        while (Outstanding < Options.Inflight && (Once ? Next < Options.Corpus : ClockNs() < Deadline)) {
            const ui32 index = Next++;
            const auto id = Blob(Options, Put ? index + Options.Corpus : index % Options.Corpus);
            if (Put) {
                Send(MakeBlobStorageProxyID(0), new TEvBlobStorage::TEvPut(id, Data, TInstant::Now() + TDuration::Seconds(120), NKikimrBlobStorage::UserData), 0, index);
            } else {
                Send(MakeBlobStorageProxyID(0), new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Now() + TDuration::Seconds(120), NKikimrBlobStorage::FastRead, Options.Workload == "restore"), 0, index);
            }
            ++Outstanding;
        }
        if (!Outstanding) {
            Promise->set_value();
            PassAway();
        }
    }
    STFUNC(StateFunc) {
        try {
            if (ev->GetTypeRewrite() == TEvBlobStorage::TEvGetResult::EventType) {
                const auto* result = ev->Get<TEvBlobStorage::TEvGetResult>();
                Y_ENSURE(result->Status == NKikimrProto::OK && result->ResponseSz == 1);
                Y_ENSURE(result->Responses[0].Status == NKikimrProto::OK && result->Responses[0].Buffer.ConvertToString() == Data, "payload mismatch");
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::TEvPutResult::EventType) {
                Y_ENSURE(ev->Get<TEvBlobStorage::TEvPutResult>()->Status == NKikimrProto::OK, "Put failed");
            } else return;
            --Outstanding;
            Issue();
        } catch (...) {
            Promise->set_exception(std::current_exception());
            PassAway();
        }
    }
};

std::map<TString, ui64> ActorTicks(TConfiguration& cfg) {
    std::map<TString, ui64> result;
    for (ui32 pool = 0; pool < 4; ++pool) {
        TExecutorPoolStats poolStats;
        TVector<TExecutorThreadStats> threads;
        cfg.ActorSystem1->GetPoolStats(pool, poolStats, threads);
        for (const auto& thread : threads) {
            for (size_t i = 0; i < Min(thread.ElapsedTicksByActivity.size(), GetActivityTypeCount()); ++i) {
                result[ToString(pool) + "/" + TString(GetActivityTypeName(i))] += thread.ElapsedTicksByActivity[i];
            }
        }
    }
    return result;
}

void Counters(TConfiguration& cfg, const TOptions& opts, TString name) {
    TFileOutput file(opts.Output + "/counters-" + name + ".txt");
    cfg.Counters->OutputPlainText(file);
}

TEvLoadTestRequest::TStorageLoad LoadConfig(const TOptions& opts) {
    TEvLoadTestRequest::TStorageLoad config;
    // Stop explicitly after our measured cutoff. The production duration timer
    // is armed before TestStartTime is assigned and can report a normal timed
    // stop as EarlyStop; do not weaken validation of its final Report instead.
    auto* profile = config.AddTablets();
    profile->SetContentType(TEvLoadTestRequest::TStorageLoad::TContentType::Validated);
    profile->SetPutHandleClass(NKikimrBlobStorage::UserData);
    profile->SetGetHandleClass(NKikimrBlobStorage::FastRead);
    auto* tablet = profile->AddTablets();
    tablet->SetTabletId(8202002);
    tablet->SetChannel(0);
    tablet->SetGroupId(0);
    tablet->SetGeneration(1);
    auto size = [&](auto* item) { item->SetMin(opts.Size); item->SetMax(opts.Size); item->SetWeight(1); };
    auto interval = [](auto* item, ui32 us) { item->SetWeight(1); item->MutableUniform()->SetMinUs(us); item->MutableUniform()->SetMaxUs(us); };
    size(profile->AddWriteSizes());
    interval(profile->AddFlushIntervals(), 1000000);
    if (opts.Workload == "put") {
        interval(profile->AddWriteIntervals(), 0);
        profile->SetMaxInFlightWriteRequests(opts.Inflight);
    } else {
        profile->SetMaxInFlightWriteRequests(0);
        profile->SetMaxInFlightReadRequests(opts.Inflight);
        size(profile->AddReadSizes());
        interval(profile->AddReadIntervals(), 0);
        auto* initial = profile->MutableInitialAllocation();
        initial->SetBlobsNumber(opts.Corpus);
        size(initial->AddBlobSizes());
        initial->SetMaxWritesInFlight(opts.Inflight);
        initial->SetPutHandleClass(NKikimrBlobStorage::UserData);
        initial->SetCollectedBlobsPerMille(0);
    }
    return config;
}

void Seed(TConfiguration& cfg, const TOptions& opts, const TString& data) {
    for (ui32 index = 0; index < opts.Corpus; ++index) {
        const auto id = Blob(opts, index);
        TString encoded = data;
        char* buffer = encoded.Detach();
        Encrypt(buffer, buffer, 0, encoded.size(), id, *cfg.GroupInfo);
        TDataPartSet parts;
        cfg.GroupInfo->Type.SplitData(static_cast<TErasureType::ECrcMode>(opts.Crc), encoded, parts);
        for (ui32 part = opts.Missing; part < cfg.GroupInfo->Type.TotalPartCount(); ++part) {
            const auto disk = cfg.GroupInfo->CreateVDiskID(cfg.GroupInfo->GetTopology().GetVDiskInSubgroup(part, id.Hash()));
            auto response = Query<TEvBlobStorage::TEvVPutResult>(cfg, cfg.GroupInfo->GetActorId(disk),
                new TEvBlobStorage::TEvVPut(TLogoBlobID(id, part + 1), parts.Parts[part].OwnedString, disk, false, nullptr,
                    TInstant::Now() + TDuration::Seconds(120), NKikimrBlobStorage::UserData));
            Y_ENSURE(response->Get<TEvBlobStorage::TEvVPutResult>()->Record.GetStatus() == NKikimrProto::OK);
        }
    }
}

void VerifyParts(TConfiguration& cfg, const TOptions& opts, const TString& data, ui32 missing = 0) {
    for (ui32 index = 0; index < opts.Corpus; ++index) {
        const auto id = Blob(opts, index);
        TString encoded = data;
        char* buffer = encoded.Detach();
        Encrypt(buffer, buffer, 0, encoded.size(), id, *cfg.GroupInfo);
        TDataPartSet parts;
        cfg.GroupInfo->Type.SplitData(static_cast<TErasureType::ECrcMode>(opts.Crc), encoded, parts);
        for (ui32 part = 0; part < cfg.GroupInfo->Type.TotalPartCount(); ++part) {
            const auto disk = cfg.GroupInfo->CreateVDiskID(cfg.GroupInfo->GetTopology().GetVDiskInSubgroup(part, id.Hash()));
            auto request = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(disk, TInstant::Now() + TDuration::Seconds(120),
                NKikimrBlobStorage::FastRead, {}, {}, {{TLogoBlobID(id, part + 1)}});
            auto response = Query<TEvBlobStorage::TEvVGetResult>(cfg, cfg.GroupInfo->GetActorId(disk), request.release());
            const auto* result = response->Get<TEvBlobStorage::TEvVGetResult>();
            Y_ENSURE(result->Record.GetStatus() == NKikimrProto::OK && result->Record.ResultSize() == 1);
            const auto& item = result->Record.GetResult(0);
            if (part < missing) {
                Y_ENSURE(item.GetStatus() == NKikimrProto::NODATA || item.GetStatus() == NKikimrProto::NOT_YET,
                    "selected missing part was repaired before measurement");
                continue;
            }
            Y_ENSURE(item.GetStatus() == NKikimrProto::OK && result->GetBlobData(item) == parts.Parts[part].OwnedString, "physical part verification failed");
        }
    }
}

void Compact(TConfiguration& cfg) {
    for (ui32 i = 0; i < cfg.VDisks->GetSize(); ++i) {
        Query<TEvCompactVDiskResult>(cfg, cfg.VDisks->Get(i).ActorID,
            TEvCompactVDisk::Create(EHullDbType::LogoBlobs, TEvCompactVDisk::EMode::FULL));
    }
}

void SeedReplicationHistory(TConfiguration& cfg, const TOptions& opts, TState& state) {
    // Model loss of previously known main payloads. Writing only the surviving
    // parts never sets the absent main's ingress bit and creates no repl work.
    // This is fixture preparation through the real durable sync-data receiver,
    // not a peer handshake or a physical disk-loss/restart measurement.
    const auto& topology = cfg.GroupInfo->GetTopology();
    NSyncLog::TNaiveFragmentWriter writer;
    for (ui32 index = 0; index < opts.Corpus; ++index) {
        const auto id = Blob(opts, index);
        TIngress ingress;
        for (ui32 part = 0; part < cfg.GroupInfo->Type.TotalPartCount(); ++part) {
            const auto disk = topology.GetVDiskInSubgroup(part, id.Hash());
            const auto historical = TIngress::CreateIngressWOLocal(&topology, disk, TLogoBlobID(id, part + 1));
            Y_ENSURE(historical);
            ingress.Merge(*historical);
        }
        char buffer[NSyncLog::MaxRecFullSize];
        // The local receiver allocates real recovery-log LSNs; these fragment
        // headers and the default SyncState do not represent a peer cursor.
        const ui32 bytes = NSyncLog::TSerializeRoutines::SetLogoBlob(cfg.GroupInfo->Type, buffer, 0, id, ingress);
        writer.Push(reinterpret_cast<const NSyncLog::TRecordHdr*>(buffer), bytes);
    }
    TString fragment;
    writer.Finish(&fragment);
    for (ui32 disk = 0; disk < cfg.VDisks->GetSize(); ++disk) {
        TActorId skeleton;
        {
            std::lock_guard lock(state.Mutex);
            skeleton = state.Disks[disk]->Skeleton;
        }
        Y_ENSURE(skeleton, "Skeleton sender was not captured during VDisk recovery");
        const auto& peer = cfg.VDisks->Get((disk + 1) % cfg.VDisks->GetSize()).VDiskID;
        auto response = Query<TEvLocalSyncDataResult>(cfg, skeleton,
            new TEvLocalSyncData(peer, TSyncState(), fragment));
        Y_ENSURE(response->Get<TEvLocalSyncDataResult>()->Status == NKikimrProto::OK,
            "historical replication ingress was not durably applied");
    }
}

void WaitForSeedIngress(TConfiguration& cfg, const TOptions& opts) {
    const auto type = cfg.GroupInfo->Type;
    const auto& topology = cfg.GroupInfo->GetTopology();
    NMatrix::TVectorType expected(0, type.TotalPartCount());
    for (ui32 part = 0; part < type.TotalPartCount(); ++part) expected.Set(part);
    const ui64 deadline = ClockNs() + 120ull * 1000000000;
    for (;;) {
        bool done = true;
        for (ui32 disk = 0; disk < cfg.VDisks->GetSize(); ++disk) {
            auto request = TEvBlobStorage::TEvVGet::CreateExtremeIndexQuery(cfg.VDisks->Get(disk).VDiskID,
                TInstant::Now() + TDuration::Seconds(120), NKikimrBlobStorage::FastRead,
                TEvBlobStorage::TEvVGet::EFlags::ShowInternals);
            for (ui32 i = 0; i < opts.Corpus; ++i) request->AddExtremeQuery(Blob(opts, i), 0, 0);
            auto response = Query<TEvBlobStorage::TEvVGetResult>(cfg, cfg.VDisks->Get(disk).ActorID, request.release());
            const auto& record = response->Get<TEvBlobStorage::TEvVGetResult>()->Record;
            Y_ENSURE(record.GetStatus() == NKikimrProto::OK);
            done &= static_cast<ui32>(record.ResultSize()) == opts.Corpus;
            std::map<TLogoBlobID, bool> seen;
            for (const auto& item : record.GetResult()) {
                const auto id = LogoBlobIDFromLogoBlobID(item.GetBlobID());
                Y_ENSURE(id.Step() && id.Step() <= opts.Corpus && id == Blob(opts, id.Step() - 1) &&
                    seen.emplace(id, true).second, "unexpected or duplicate seeded index result");
                const TIngress ingress(item.GetIngress());
                done &= item.HasIngress() && ingress.PartsWeKnowAbout(type) == expected;
                // Check every historical placement bit, not only its union.
                for (ui32 order = 0; order < type.BlobSubgroupSize(); ++order) {
                    NMatrix::TVectorType required(0, type.TotalPartCount());
                    if (order < type.TotalPartCount()) required.Set(order);
                    done &= ingress.KnownParts(type, order) == required;
                }
                const ui32 self = topology.GetIdxInSubgroup(cfg.VDisks->Get(disk).VDiskID, id.Hash());
                NMatrix::TVectorType local(0, type.TotalPartCount());
                if (opts.Missing <= self && self < type.TotalPartCount()) local.Set(self);
                done &= ingress.LocalParts(type) == local;
            }
        }
        if (done) return;
        Y_ENSURE(ClockNs() < deadline, "full historical ingress and exact surviving local parts were not visible before replication");
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

using TRepairParts = TVector<TVector<TRope>>;
TRepairParts PrepareRepairVerification(TConfiguration& cfg, const TOptions& opts, const TString& data) {
    TRepairParts result(opts.Corpus);
    for (ui32 index = 0; index < opts.Corpus; ++index) {
        const auto id = Blob(opts, index);
        TString encoded = data;
        char* buffer = encoded.Detach();
        Encrypt(buffer, buffer, 0, encoded.size(), id, *cfg.GroupInfo);
        TDataPartSet parts;
        cfg.GroupInfo->Type.SplitData(static_cast<TErasureType::ECrcMode>(opts.Crc), encoded, parts);
        for (ui32 part = 0; part < opts.Missing; ++part) result[index].push_back(parts.Parts[part].OwnedString);
    }
    return result;
}

TRepairParts PrepareRestoreVerification(TConfiguration& cfg, const TOptions& opts, const TString& data) {
    auto allParts = opts;
    allParts.Missing = cfg.GroupInfo->Type.TotalPartCount();
    return PrepareRepairVerification(cfg, allParts, data);
}

struct TRestoreLayoutCheck {
    bool ResponsesValid = true;
    bool PayloadsValid = true;
    bool Full = false;
    ui32 DistinctParts = 0;
    ui32 EffectiveReplicas = 0;
    ui64 Requests = 0;
    ui64 PayloadBytes = 0;
    ui64 RecoveredBytes = 0;
    ui64 MainCopies = 0;
    ui64 HandoffCopies = 0;
    ui64 RecoveredWithMain = 0;
    ui64 RecoveredWithHandoff = 0;
    ui64 RecoveredHandoffOnly = 0;
    NJson::TJsonValue Trace{NJson::JSON_MAP};

    bool Success() const { return ResponsesValid && PayloadsValid && Full; }
};

TRestoreLayoutCheck ReadRestoreLayout(TConfiguration& cfg, const TOptions& opts, ui32 index,
        const TVector<TRope>& expected) {
    // MustRestoreFirst may acknowledge a full layout using handoffs when a
    // main is slow. Read every physical copy, including redundant copies; a
    // union of P part types on fewer than P eligible disks is insufficient.
    const auto& info = *cfg.GroupInfo;
    const auto& type = info.Type;
    const auto& topology = info.GetTopology();
    const auto id = Blob(opts, index);
    Y_ENSURE(expected.size() == type.TotalPartCount());
    TRestoreLayoutCheck check;
    TSubgroupPartLayout layout;
    TVector<ui64> missingBytes(opts.Missing, 0);
    check.Trace["blob_id"] = id.ToString();
    for (ui32 subgroup = 0; subgroup < type.BlobSubgroupSize(); ++subgroup) {
        const auto disk = info.CreateVDiskID(topology.GetVDiskInSubgroup(subgroup, id.Hash()));
        const ui32 order = info.GetOrderNumber(disk);
        auto request = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(disk,
            TInstant::Now() + TDuration::Seconds(120), NKikimrBlobStorage::FastRead);
        request->AddExtremeQuery(id, 0, 0);
        auto response = Query<TEvBlobStorage::TEvVGetResult>(cfg, info.GetActorId(disk), request.release());
        ++check.Requests;
        const auto* result = response->Get<TEvBlobStorage::TEvVGetResult>();
        const auto& record = result->Record;
        NJson::TJsonValue diskTrace(NJson::JSON_MAP);
        diskTrace["subgroup_order"] = subgroup;
        diskTrace["physical_order"] = order;
        diskTrace["vdisk_id"] = disk.ToString();
        diskTrace["handoff"] = subgroup >= type.TotalPartCount();
        diskTrace["verification_requests"] = 1;
        diskTrace["status"] = NKikimrProto::EReplyStatus_Name(record.GetStatus());
        const bool envelopeValid = record.GetStatus() == NKikimrProto::OK && record.ResultSize() &&
            VDiskIDFromVDiskID(record.GetVDiskID()) == disk;
        check.ResponsesValid &= envelopeValid;
        diskTrace["response_valid"] = envelopeValid;
        ui32 seen = 0;
        ui64 diskBytes = 0;
        for (const auto& item : record.GetResult()) {
            const auto itemId = LogoBlobIDFromLogoBlobID(item.GetBlobID());
            const ui32 partId = itemId.PartId();
            const bool idValid = itemId.FullID() == id && partId <= type.TotalPartCount();
            const bool absent = item.GetStatus() == NKikimrProto::NODATA || item.GetStatus() == NKikimrProto::NOT_YET;
            NJson::TJsonValue copy(NJson::JSON_MAP);
            copy["blob_id"] = itemId.ToString();
            copy["part_id"] = partId;
            copy["status"] = NKikimrProto::EReplyStatus_Name(item.GetStatus());
            copy["id_valid"] = idValid;
            check.ResponsesValid &= idValid && (absent || item.GetStatus() == NKikimrProto::OK);
            if (item.GetStatus() == NKikimrProto::OK) {
                const TRope payload = result->GetBlobData(item);
                diskBytes += payload.size();
                copy["payload_bytes"] = payload.size();
                const bool eligible = idValid && partId &&
                    (subgroup >= type.TotalPartCount() || partId == subgroup + 1);
                const bool unique = partId && partId <= type.TotalPartCount() && !(seen & (1u << (partId - 1)));
                const bool payloadValid = eligible && payload == expected[partId - 1];
                copy["eligible"] = eligible;
                copy["unique_on_vdisk"] = unique;
                copy["payload_valid"] = payloadValid;
                check.ResponsesValid &= eligible && unique;
                check.PayloadsValid &= payloadValid;
                if (partId && partId <= type.TotalPartCount()) seen |= 1u << (partId - 1);
                if (envelopeValid && eligible && unique && payloadValid) {
                    layout.AddItem(subgroup, partId - 1, type);
                    if (subgroup < type.TotalPartCount()) ++check.MainCopies;
                    else ++check.HandoffCopies;
                    if (partId <= opts.Missing) {
                        missingBytes[partId - 1] = payload.size();
                        NJson::TJsonValue destination(NJson::JSON_MAP);
                        destination["part_id"] = partId;
                        destination["subgroup_order"] = subgroup;
                        destination["physical_order"] = order;
                        destination["handoff"] = subgroup >= type.TotalPartCount();
                        destination["payload_bytes"] = payload.size();
                        check.Trace["recovered_destinations"].AppendValue(std::move(destination));
                    }
                }
            }
            diskTrace["copies"].AppendValue(std::move(copy));
        }
        diskTrace["successful_payload_bytes"] = diskBytes;
        check.PayloadBytes += diskBytes;
        check.Trace["disks"].AppendValue(std::move(diskTrace));
    }
    const TBlobStorageGroupInfo::TSubgroupVDisks failed(&topology);
    check.Full = info.GetQuorumChecker().GetBlobState(layout, failed) == TBlobStorageGroupInfo::EBS_FULL;
    check.DistinctParts = layout.CountDistinctParts(type);
    check.EffectiveReplicas = layout.CountEffectiveReplicas(type);
    for (ui32 part = 0; part < opts.Missing; ++part) {
        check.RecoveredBytes += missingBytes[part];
        const ui32 destinations = layout.GetDisksWithPart(part);
        const bool main = destinations & (1u << part);
        const bool handoff = destinations >> type.TotalPartCount();
        check.RecoveredWithMain += main;
        check.RecoveredWithHandoff += handoff;
        check.RecoveredHandoffOnly += handoff && !main;
    }
    check.Trace["responses_valid"] = check.ResponsesValid;
    check.Trace["payloads_valid"] = check.PayloadsValid;
    check.Trace["full_effective_layout"] = check.Full;
    check.Trace["distinct_verified_parts"] = check.DistinctParts;
    check.Trace["effective_replicas"] = check.EffectiveReplicas;
    check.Trace["verified_layout"] = layout.ToString(type);
    check.Trace["verification_requests"] = check.Requests;
    check.Trace["verification_payload_bytes"] = check.PayloadBytes;
    check.Trace["recovered_part_bytes"] = check.RecoveredBytes;
    check.Trace["verified_main_copies"] = check.MainCopies;
    check.Trace["verified_handoff_copies"] = check.HandoffCopies;
    check.Trace["recovered_part_types_with_main_copy"] = check.RecoveredWithMain;
    check.Trace["recovered_part_types_with_handoff_copy"] = check.RecoveredWithHandoff;
    check.Trace["recovered_part_types_handoff_only"] = check.RecoveredHandoffOnly;
    check.Trace["success"] = check.Success();
    return check;
}

bool CheckRestoreCompletion(TConfiguration& cfg, const TOptions& opts, const TRepairParts& expected,
        TState* measured, NJson::TJsonValue& trace) {
    bool success = true;
    ui64 requests = 0, payloadBytes = 0, recoveredBytes = 0;
    ui64 mainCopies = 0, handoffCopies = 0, recoveredWithMain = 0, recoveredWithHandoff = 0, recoveredHandoffOnly = 0;
    const ui64 begin = ClockNs();
    const ui64 cpuBegin = ClockNs(CLOCK_PROCESS_CPUTIME_ID);
    for (ui32 index = 0; index < opts.Corpus; ++index) {
        auto check = ReadRestoreLayout(cfg, opts, index, expected[index]);
        success &= check.Success();
        requests += check.Requests;
        payloadBytes += check.PayloadBytes;
        recoveredBytes += check.RecoveredBytes;
        mainCopies += check.MainCopies;
        handoffCopies += check.HandoffCopies;
        recoveredWithMain += check.RecoveredWithMain;
        recoveredWithHandoff += check.RecoveredWithHandoff;
        recoveredHandoffOnly += check.RecoveredHandoffOnly;
        if (measured) {
            measured->RepairVerificationRequests += check.Requests;
            measured->RepairVerificationBytes += check.PayloadBytes;
        }
        // Keep completed-blob diagnostics even if a subsequent query fails.
        trace["blobs"].AppendValue(std::move(check.Trace));
    }
    trace["wall_ns"] = ClockNs() - begin;
    trace["process_cpu_ns"] = ClockNs(CLOCK_PROCESS_CPUTIME_ID) - cpuBegin;
    trace["verification_requests"] = requests;
    trace["verification_payload_bytes"] = payloadBytes;
    trace["recovered_part_bytes"] = recoveredBytes;
    trace["verified_main_copies"] = mainCopies;
    trace["verified_handoff_copies"] = handoffCopies;
    trace["recovered_part_types_with_main_copy"] = recoveredWithMain;
    trace["recovered_part_types_with_handoff_copy"] = recoveredWithHandoff;
    trace["recovered_part_types_handoff_only"] = recoveredHandoffOnly;
    trace["success"] = success;
    if (success && measured) measured->ObservedRecoveredPartBytes = recoveredBytes;
    return success;
}

void VerifyRestoreOracle(TConfiguration& cfg, const TOptions& opts, NJson::TJsonValue& trace) {
    auto probe = opts;
    probe.Tablet += 200;
    probe.Size = 4096;
    probe.Corpus = 4;
    probe.Missing = 2;
    probe.Crc = TErasureType::CrcModeNone;
    const auto expected = PrepareRestoreVerification(cfg, probe, TString(probe.Size, 'R'));
    const ui32 parts = cfg.GroupInfo->Type.TotalPartCount();
    const std::array<TString, 4> names = {"full_main", "valid_handoff", "dense_handoff_incomplete", "corrupt_redundant_copy"};
    for (ui32 index = 0; index < probe.Corpus; ++index) {
        const auto id = Blob(probe, index);
        auto put = [&](ui32 part, ui32 subgroup, const TRope& payload) {
            const auto disk = cfg.GroupInfo->CreateVDiskID(cfg.GroupInfo->GetTopology().GetVDiskInSubgroup(subgroup, id.Hash()));
            auto response = Query<TEvBlobStorage::TEvVPutResult>(cfg, cfg.GroupInfo->GetActorId(disk),
                new TEvBlobStorage::TEvVPut(TLogoBlobID(id, part + 1), payload, disk, false, nullptr,
                    TInstant::Now() + TDuration::Seconds(120), NKikimrBlobStorage::UserData));
            Y_ENSURE(response->Get<TEvBlobStorage::TEvVPutResult>()->Record.GetStatus() == NKikimrProto::OK,
                "restore oracle probe write failed");
        };
        for (ui32 part = 0; part < parts; ++part) {
            const bool handoff = (index == 1 && part == 0) || (index == 2 && part < 2);
            put(part, handoff ? parts : part, expected[index][part]);
        }
        if (index == 3) {
            TString corrupt = expected[index][0].ConvertToString();
            corrupt.Detach()[0] ^= 1;
            put(0, parts, TRope(corrupt));
        }
        auto check = ReadRestoreLayout(cfg, probe, index, expected[index]);
        const bool passed = index < 2 ? check.Success() && check.EffectiveReplicas == parts &&
                check.HandoffCopies == (index == 1 ? 1u : 0u) && check.RecoveredHandoffOnly == (index == 1 ? 1u : 0u)
            : index == 2 ? !check.Success() && check.ResponsesValid && check.PayloadsValid &&
                check.DistinctParts == parts && check.EffectiveReplicas == parts - 1 && !check.Full
            : !check.Success() && check.ResponsesValid && !check.PayloadsValid && check.Full;
        check.Trace["case"] = names[index];
        check.Trace["expected_success"] = index < 2;
        check.Trace["self_check_passed"] = passed;
        trace["cases"].AppendValue(std::move(check.Trace));
        Y_ENSURE(passed, "restore layout oracle self-check failed: " << names[index]);
    }
    trace["retained_probe_blobs"] = probe.Corpus;
    trace["probe_blob_size"] = probe.Size;
    trace["probe_crc"] = probe.Crc;
    trace["probe_tablet"] = probe.Tablet;
    trace["probe_logical_bytes"] = ui64(probe.Corpus) * probe.Size;
    ui64 probeBytes = expected[3][0].size(); // additional corrupt redundant copy
    for (const auto& blobParts : expected) {
        for (const auto& part : blobParts) probeBytes += part.size();
    }
    trace["retained_physical_payload_bytes"] = probeBytes;
    trace["success"] = true;
}

bool CheckRepairCompletion(TConfiguration& cfg, const TOptions& opts, const TRepairParts& expected, TState& state) {
    ui64 recoveredBytes = 0;
    for (ui32 index = 0; index < opts.Corpus; ++index) {
        const auto id = Blob(opts, index);
        for (ui32 part = 0; part < opts.Missing; ++part) {
            const auto disk = cfg.GroupInfo->CreateVDiskID(cfg.GroupInfo->GetTopology().GetVDiskInSubgroup(part, id.Hash()));
            auto request = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(disk,
                TInstant::Now() + TDuration::Seconds(120), NKikimrBlobStorage::FastRead, {}, {}, {{TLogoBlobID(id, part + 1)}});
            auto response = Query<TEvBlobStorage::TEvVGetResult>(cfg, cfg.GroupInfo->GetActorId(disk), request.release());
            ++state.RepairVerificationRequests;
            const auto* result = response->Get<TEvBlobStorage::TEvVGetResult>();
            Y_ENSURE(result->Record.GetStatus() == NKikimrProto::OK && result->Record.ResultSize() == 1);
            const auto& item = result->Record.GetResult(0);
            if (item.GetStatus() == NKikimrProto::NODATA || item.GetStatus() == NKikimrProto::NOT_YET) return false;
            Y_ENSURE(item.GetStatus() == NKikimrProto::OK && result->GetBlobData(item) == expected[index][part],
                "repaired physical part payload mismatch");
            state.RepairVerificationBytes += result->GetBlobData(item).size();
            recoveredBytes += result->GetBlobData(item).size();
        }
    }
    state.ObservedRecoveredPartBytes = recoveredBytes;
    return true;
}

void PrepareScrubFaults(TConfiguration& cfg, const TOptions& opts, TState& state) {
    Compact(cfg);
    using T = TEvBlobStorage::TEvCaptureVDiskLayoutResult;
    ui32 found = 0;
    for (ui32 disk = 0; disk < cfg.VDisks->GetSize(); ++disk) {
        auto response = Query<T>(cfg, cfg.VDisks->Get(disk).ActorID, new TEvBlobStorage::TEvCaptureVDiskLayout);
        for (const auto& item : response->Get<T>()->Layout) {
            if (item.Database != T::EDatabase::LogoBlobs || item.RecordType == T::ERecordType::IndexRecord ||
                    item.BlobId.TabletID() != opts.Tablet) continue;
            for (ui32 part = 0; part < opts.Missing; ++part) {
                const auto id = cfg.GroupInfo->CreateVDiskID(cfg.GroupInfo->GetTopology().GetVDiskInSubgroup(part, item.BlobId.Hash()));
                if (id == cfg.VDisks->Get(disk).VDiskID) {
                    state.Faults.push_back({disk, item.Location.ChunkIdx, item.Location.Offset, item.Location.Offset + item.Location.Size});
                    ++found;
                }
            }
        }
    }
    Y_ENSURE(found == opts.Corpus * opts.Missing, "each selected main part must have exactly one persisted extent");
    state.FaultsEnabled.store(true, std::memory_order_release);
}

void RunMaintenance(TConfiguration& cfg, const TOptions& opts, TState& state, const TRepairParts& expected) {
    if (opts.Workload == "replication") {
        for (ui32 i = 0; i < cfg.VDisks->GetSize(); ++i) {
            cfg.ActorSystem1->Send(new IEventHandle(TEvBlobStorage::EvCommenceRepl, 0,
                cfg.VDisks->Get(i).ActorID, {}, nullptr, 0));
        }
    } else {
        state.ScrubEnabled = true;
    }
    const ui64 deadline = ClockNs() + 300ull * 1000000000;
    ui32 sample = 0;
    while (true) {
        bool done = true;
        if (opts.Workload == "replication") {
            for (ui32 i = 0; i < cfg.VDisks->GetSize(); ++i) {
                auto response = Query<TEvBlobStorage::TEvVStatusResult>(cfg, cfg.VDisks->Get(i).ActorID,
                    new TEvBlobStorage::TEvVStatus(cfg.VDisks->Get(i).VDiskID));
                const auto& record = response->Get<TEvBlobStorage::TEvVStatusResult>()->Record;
                done &= record.GetStatus() == NKikimrProto::OK && record.GetReplicated();
            }
        } else {
            for (const auto& fault : state.Faults) done &= state.Disks[fault.Disk]->ScrubSuccess.load() >= 2;
        }
        if (done && CheckRepairCompletion(cfg, opts, expected, state)) return;
        Y_ENSURE(ClockNs() < deadline, "maintenance did not complete within 300 seconds");
        std::this_thread::sleep_for(std::chrono::seconds(1));
        Counters(cfg, opts, ToString(++sample));
    }
}

void VerifyInstrumentation(TConfiguration& cfg, const TOptions& opts, TState& state) {
    auto probe = opts;
    probe.Corpus = 1;
    probe.Missing = 0;
    // Exercise the cell's real part size and Huge/inline classification before
    // timing; a tiny probe would miss the helper's reduced VPut size limit.
    probe.Size = opts.Size;
    probe.Tablet += 100;
    const TString data(probe.Size, 'v');
    state.Measuring = true;
    Seed(cfg, probe, data);
    VerifyParts(cfg, probe, data);
    ui64 written = 0, read = 0, gets = 0;
    const ui64 expected = cfg.GroupInfo->Type.PartSize(TLogoBlobID(Blob(probe, 0), 1)) * cfg.GroupInfo->Type.TotalPartCount();
    const ui64 deadline = ClockNs() + 5ull * 1000000000;
    // Some result paths notify SkeletonFront after replying to the caller.
    // Keep collection enabled until every known completion has been observed.
    do {
        written = read = gets = 0;
        for (const auto& disk : state.Disks) {
            written += disk->VWriteBytes;
            read += disk->VReadBytes;
            gets += disk->VGet;
        }
        if (written == expected && read == expected && gets == cfg.GroupInfo->Type.TotalPartCount()) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    } while (ClockNs() < deadline);
    state.Measuring = false;
    Y_ENSURE(written == expected && read == expected && gets == cfg.GroupInfo->Type.TotalPartCount(),
        "VDisk payload instrumentation failed: " << written << "/" << read << "/" << gets << " expected " << expected);
    state.ResetMetrics();
}

constexpr ui32 BenchmarkChunkSize = 16u << 20;
constexpr ui32 BenchmarkMaxPartSize = 4u << 20;

std::pair<ui32, ui32> CheckHugeGeometry(const TVDiskConfig& config, ui32 chunkSize) {
    // MkManyTmp formats 4 KiB sectors with plainDataChunks=false. Use its exact
    // append payload and the requested chunk size, a lower bound on the
    // formatted user chunk. Local recovery starts chains at one append block.
    constexpr ui32 appendBlock = (4u << 10) - sizeof(NPDisk::TDataSectorFooter) - NPDisk::CanarySize;
    const ui32 maximum = config.MaxLogoBlobDataSize + TDiskBlob::HeaderSize;
    NHuge::NPrivate::TChainLayoutBuilder builder(1, config.MilestoneHugeBlobInBytes / appendBlock,
        (maximum + appendBlock - 1) / appendBlock, config.HugeBlobOverhead);
    ui32 largestSlot = 0, minimumSlots = Max<ui32>();
    for (const auto& segment : builder.GetLayout()) {
        const ui32 slots = (chunkSize / appendBlock) / segment.Right;
        Y_ENSURE(slots > 1 && slots <= 32768, "benchmark Huge class must fit 2..32768 slots: "
            << "slot bytes " << segment.Right * appendBlock << ", slots " << slots);
        largestSlot = Max(largestSlot, segment.Right * appendBlock);
        minimumSlots = Min(minimumSlots, slots);
    }
    return {largestSlot, minimumSlots};
}

int Run(const TOptions& opts) {
    TFsPath(opts.Output).MkDirs();
    VerifyLoadPayloadValidator();
    const auto erasure = opts.Species == "block-8-2" ? TErasureType::Erasure8Plus2Block : TErasureType::Erasure4Plus2Block;
    TBlobStorageGroupType type(erasure);
    const ui32 disks = type.BlobSubgroupSize();
    Y_ENSURE(type.PartSize(TLogoBlobID(Blob(opts, 0), 1)) <= BenchmarkMaxPartSize,
        "requested encoded part exceeds the benchmark VDisk limit");
    TState state(disks);
    NJson::TJsonValue result(NJson::JSON_MAP);
    TFailureDiagnostics failureDiagnostics(opts.Output, state, result);
    // Open inherited counters before the actor system creates worker threads.
    TProcessPmu pmu;
    TConfiguration cfg(TAllPDisksConfiguration::MkManyTmp(disks, BenchmarkChunkSize, opts.DiskSize, "ROT"), disks, 1, erasure);
    TDefaultVDiskSetup setup;
    setup.AddConfigModifier([&](TVDiskConfig* config) {
        // 10 MiB logical Block42 parts plus CRC fit 4 MiB. A 10 MiB physical
        // limit creates Huge classes with only one slot in these small chunks.
        config->MaxLogoBlobDataSize = BenchmarkMaxPartSize;
        config->MinHugeBlobInBytes = 64u << 10;
        config->RunScrubber = opts.Workload == "scrub";
        config->ReplPausedAtStart = opts.Workload == "replication";
        CheckHugeGeometry(*config, BenchmarkChunkSize);
    });
    cfg.BeforeActorSystemStart = [&](TActorSystemSetup& system) {
        for (auto& [id, command] : system.LocalServices) {
            for (ui32 i = 0; i < disks; ++i) {
                if (id == cfg.VDisks->Get(i).ActorID || id == cfg.PDisks->Get(i + 1).PDiskActorID) {
                    command.Actor.reset(new TDiskObserver(THolder<IActor>(command.Actor.release()), state, i,
                        id == cfg.PDisks->Get(i + 1).PDiskActorID));
                    break;
                }
            }
        }
        system.LocalServices.emplace_back(MakeBlobStorageNodeWardenID(1),
            TActorSetupCmd(new TScrubScheduler(state), TMailboxType::Revolving, 0));
    };
    cfg.Prepare(&setup, true, opts.Workload == "replication");
    cfg.DbInitWait();
    if (opts.Workload == "replication") {
        std::lock_guard lock(state.Mutex);
        for (const auto& disk : state.Disks) {
            Y_ENSURE(disk->Skeleton, "VDisk recovery did not expose its Skeleton sender");
        }
    }
    TIntrusivePtr<TDsProxyNodeMon> nodeMon = new TDsProxyNodeMon(cfg.Counters, true);
    TIntrusivePtr<TStoragePoolCounters> pool = new TStoragePoolCounters(cfg.Counters, "benchmark", NPDisk::DEVICE_TYPE_ROT);
    const auto proxy = cfg.ActorSystem1->Register(CreateBlobStorageGroupProxyConfigured(TIntrusivePtr(cfg.GroupInfo), nullptr,
        true, nodeMon, std::move(pool), TBlobStorageProxyParameters{
            .Controls = TBlobStorageProxyControlWrappers{
                .EnablePutBatching = TControlWrapper(DefaultEnablePutBatching, false, true),
                .EnableVPatch = TControlWrapper(DefaultEnableVPatch, false, true),
            },
        }));
    const bool load = opts.Crc == TErasureType::CrcModeNone && opts.Missing == 0 && (opts.Workload == "put" || opts.Workload == "get");
    const auto observer = cfg.ActorSystem1->Register(new TGroupObserver(proxy, state, opts.Workload != "put", load,
        load ? 8202002 : opts.Tablet, opts.Size));
    cfg.ActorSystem1->RegisterLocalService(MakeBlobStorageProxyID(0), observer);
    Counters(cfg, opts, "empty");
    VerifyInstrumentation(cfg, opts, state);
    Counters(cfg, opts, "probe");

    result["schema"] = 1;
    result["species"] = opts.Species;
    result["workload"] = opts.Workload;
    result["blob_size"] = opts.Size;
    result["inflight"] = opts.Inflight;
    result["crc"] = opts.Crc;
    result["encryption_mode"] = TBlobStorageGroupInfo::PrintEncryptionMode(cfg.GroupInfo->GetEncryptionMode());
    result["missing_data_parts"] = opts.Missing;
    result["corpus_blobs"] = opts.Corpus;
    result["logical_domains"] = disks;
    result["physical_pdisks"] = disks;
    result["disk_size"] = opts.DiskSize;
    result["chunk_size"] = BenchmarkChunkSize;
    result["physical_huge_threshold"] = 64u << 10;
    result["max_vdisk_blob_size"] = cfg.VDisks->Get(0).Cfg->MaxLogoBlobDataSize;
    result["configured_device_type"] = "ROT (temporary regular file)";
    result["proxy_put_batching"] = DefaultEnablePutBatching;
    result["proxy_vpatch"] = DefaultEnableVPatch;
    result["instrumentation_preflight"] = "exact P VGet requests and P*PartSize VDisk payload bytes in each direction";
    result["instrumentation_probe_blob_size"] = opts.Size;
    result["instrumentation_probe_crc"] = opts.Crc;
    result["instrumentation_retained_probe_blobs"] = 1;
    result["instrumentation_probe_tablet"] = opts.Tablet + 100;
    result["load_actor_sentinel_blobs"] = load ? 1 : 0;
    result["load_actor_sentinel_blob_size"] = load ? 1 : 0;
    result["corpus_accounting"] = load
        ? "LoadActor corpus plus one retained cell-sized probe on a separate tablet, one 1-byte LoadActor sentinel at maximum step, and block/barrier/keep metadata; probe-to-begin includes control metadata"
        : "Custom-driver corpus plus one retained cell-sized probe on a separate tablet and control metadata";
    result["inter_host_wire_bytes"] = 0;
    result["limitations"] = "One host; shared filesystem and CPU; no inter-host network. PDisk request bytes include page padding but not device-sector framing. Source/binary provenance is collected by the outer process runner; PMU availability and scope are reported per counter.";
    result["executor_threads"] = "basic:8/8/8;io:10";
    for (ui32 i = 0; i < disks; ++i) {
        result["temporary_files"].AppendValue(cfg.PDisks->Get(i + 1).Filename);
        const auto& config = *cfg.VDisks->Get(i).Cfg;
        Y_ENSURE(config.MaxLogoBlobDataSize == BenchmarkMaxPartSize,
            "benchmark VDisk limit differs between disks");
        const auto [largestSlot, minimumSlots] = CheckHugeGeometry(config, cfg.PDisks->Get(i + 1).ChunkSize);
        NJson::TJsonValue effective(NJson::JSON_MAP);
        effective["order"] = i;
        effective["max_logo_blob_data_size"] = config.MaxLogoBlobDataSize;
        effective["min_huge_blob_in_bytes"] = config.MinHugeBlobInBytes;
        effective["milestone_huge_blob_in_bytes"] = config.MilestoneHugeBlobInBytes;
        effective["huge_blob_overhead"] = config.HugeBlobOverhead;
        effective["largest_huge_slot_bytes"] = largestSlot;
        effective["minimum_huge_slots_per_requested_chunk"] = minimumSlots;
        effective["configured_add_header"] = config.AddHeader;
        effective["effective_add_header"] = config.AddHeader && type.CanUseLegacyHeader();
        result["effective_vdisk_config"].AppendValue(std::move(effective));
    }

    std::future<std::unique_ptr<IEventHandle>> loadResult;
    TActorId loadParent;
    std::future<void> customResult;
    TString data;
    TRepairParts repairParts;
    if (opts.Workload == "restore") {
        result["restore_oracle_version"] = 2;
        result["restore_verification_policy"] = "Full byte-verified effective P layout over all main and handoff VDisks, using GetBlobState(EBS_FULL) with no failed disks; all returned copies must be valid";
        result["restore_interval_semantics"] = "MustRestoreFirst requests plus one complete physical-layout readback before cutoff. Readback requests, all successful payload bytes, CPU and trace construction are included in interval totals; verification traffic is reported separately and is not DSProxy repair traffic. Final readback and trace-file serialization follow cutoff.";
        result["corpus_accounting"] = "Custom-driver corpus, one retained cell-sized instrumentation probe, four retained 4KiB CRCNone restore-oracle probes on separate tablets, and control metadata; oracle probes include one extra corrupt redundant physical copy";
        VerifyRestoreOracle(cfg, opts, result["restore_oracle_preflight"]);
        TFileOutput output(opts.Output + "/restore-oracle-preflight.json");
        NJson::WriteJson(&output, &result["restore_oracle_preflight"], true);
    }
    if (load) {
        auto config = LoadConfig(opts);
        TFileOutput(opts.Output + "/load.pb.txt").Write(config.DebugString());
        auto promise = std::make_shared<TResultPromise>();
        loadResult = promise->get_future();
        loadParent = cfg.ActorSystem1->Register(new TLoadParent(config, cfg.Counters, promise, state));
        const ui64 deadline = ClockNs() + 300ull * 1000000000;
        while (!state.FirstRequestNs) {
            Y_ENSURE(ClockNs() < deadline, "LoadActor did not start");
            if (loadResult.wait_for(std::chrono::milliseconds(10)) == std::future_status::ready) {
                auto response = loadResult.get();
                ythrow yexception() << "LoadActor finished before starting: " << response->Get<TEvLoad::TEvLoadTestFinished>()->ErrorReason;
            }
        }
        std::this_thread::sleep_for(std::chrono::seconds(opts.Warmup));
    } else {
        data.resize(opts.Size);
        for (ui32 i = 0; i < opts.Size; ++i) data.Detach()[i] = char((i * 131u + i / 31 + 82) & 255);
        auto seed = opts;
        if (opts.Workload == "scrub") seed.Missing = 0;
        Seed(cfg, seed, data);
        if (opts.Workload == "replication") SeedReplicationHistory(cfg, opts, state);
        auto warm = opts;
        warm.Workload = "get";
        warm.Seconds = Max(1u, opts.Warmup);
        auto promise = std::make_shared<std::promise<void>>();
        auto future = promise->get_future();
        cfg.ActorSystem1->Register(new TDriver(warm, data, promise, false, false));
        Y_ENSURE(future.wait_for(std::chrono::seconds(warm.Seconds + 125)) == std::future_status::ready);
        future.get();
        if (opts.Workload == "replication") {
            WaitForSeedIngress(cfg, opts);
        } else if (opts.Workload == "scrub") {
            Y_ENSURE(opts.Missing, "scrub requires --missing 1 or 2");
            VerifyParts(cfg, opts, data);
            PrepareScrubFaults(cfg, opts, state);
        }
        if (opts.Workload != "scrub") VerifyParts(cfg, opts, data, opts.Missing);
        if (opts.Workload == "replication") {
            result["replication_history_validated_index_records"] = ui64(disks) * opts.Corpus;
            result["replication_history_known_main_parts_per_blob"] = type.TotalPartCount();
            result["replication_history_absent_main_parts_per_blob"] = opts.Missing;
            result["replication_history_preflight"] = "every exact corpus ID on every VDisk: all main known bits, no handoff known bits, exact surviving local mask; every missing main data query NODATA/NOT_YET";
        }
        if (opts.Workload == "restore") {
            repairParts = PrepareRestoreVerification(cfg, opts, data);
        } else if (opts.Workload == "replication" || opts.Workload == "scrub") {
            repairParts = PrepareRepairVerification(cfg, opts, data);
        }
    }
    result["isa_l_encode_dispatcher"] = erasure == TErasureType::Erasure8Plus2Block
        ? ObservedIsaLEncodeDispatcher() : "not used by block-4-2";
    Counters(cfg, opts, "begin");
    const auto actorBegin = ActorTicks(cfg);
    const ui64 cpuBegin = ClockNs(CLOCK_PROCESS_CPUTIME_ID);
    const ui64 begin = ClockNs();
    pmu.Begin();
    {
        std::lock_guard lock(state.Mutex);
        state.BeginNs = begin;
        state.Measuring = true;
    }
    const bool maintenance = opts.Workload == "replication" || opts.Workload == "scrub";
    if (!load && !maintenance) {
        auto promise = std::make_shared<std::promise<void>>();
        customResult = promise->get_future();
        cfg.ActorSystem1->Register(new TDriver(opts, data, promise, opts.Workload == "restore", opts.Workload == "put"));
    }
    ui32 sample = 0;
    if (maintenance) {
        RunMaintenance(cfg, opts, state, repairParts);
    } else if (load) {
        for (; sample < opts.Seconds; ++sample) {
            std::this_thread::sleep_until(std::chrono::steady_clock::time_point(std::chrono::nanoseconds(begin + ui64(sample + 1) * 1000000000)));
            Y_ENSURE(!state.LoadFinishedNs, "LoadActor finished before measurement cutoff");
            Y_ENSURE(state.ValidationErrors == 0, "matching request failed or LoadActor payload mismatch");
            Counters(cfg, opts, ToString(sample + 1));
        }
    } else {
        while (customResult.wait_for(std::chrono::seconds(1)) != std::future_status::ready) {
            Counters(cfg, opts, ToString(++sample));
            Y_ENSURE(ClockNs() - begin < ui64(opts.Seconds + 125) * 1000000000, "workload timeout");
        }
        customResult.get();
        if (opts.Workload == "restore") {
            result["restore_requests_wall_ns"] = ClockNs() - begin;
            result["restore_requests_process_cpu_ns"] = ClockNs(CLOCK_PROCESS_CPUTIME_ID) - cpuBegin;
            Y_ENSURE(CheckRestoreCompletion(cfg, opts, repairParts, &state, result["restore_layout_timed"]),
                "MustRestoreFirst did not leave a byte-verified full effective layout; see restore_layout_timed");
        }
    }
    {
        std::lock_guard lock(state.Mutex);
        state.Measuring = false;
        state.MeasurementFinished = true;
        result["requests_inflight_at_cutoff"] = state.Outstanding;
        result["completed_requests_started_before_window"] = state.BeganBeforeWindow;
    }
    const ui64 end = ClockNs();
    const ui64 cpuEnd = ClockNs(CLOCK_PROCESS_CPUTIME_ID);
    Y_ENSURE(!load || !state.LoadFinishedNs || state.LoadFinishedNs >= end,
        "LoadActor finished before measurement cutoff");
    result["pmu"] = pmu.End();
    const auto actorEnd = ActorTicks(cfg);
    Counters(cfg, opts, "end");
    if (load) {
        result["load_stop_method"] = "explicit parent-forwarded PoisonPill after measurement and end snapshots; no LoadActor duration timer";
        cfg.ActorSystem1->Send(new IEventHandle(loadParent, TActorId(), new TEvents::TEvPoisonPill));
    }
    result["wall_ns"] = end - begin;
    result["process_cpu_ns"] = cpuEnd - cpuBegin;
    result["requests"] = state.Requests.load();
    result["logical_bytes"] = state.LogicalBytes.load();
    result["latency_window_semantics"] = "All matching completions inside the measured interval; full request latency including starts before the interval. Requests still outstanding at cutoff are counted separately and have no completion latency sample. CPU and VDisk/PDisk event bytes are interval totals, not a drained-request cohort.";
    if (maintenance) {
        result["logical_bytes"] = ui64(opts.Size) * opts.Corpus;
    }
    if (maintenance || opts.Workload == "restore") {
        result["repaired_blobs"] = opts.Corpus;
        if (opts.Workload == "restore") {
            result["repaired_part_types"] = opts.Corpus * opts.Missing;
        } else {
            result["repaired_main_parts"] = opts.Corpus * opts.Missing;
        }
        const ui64 expectedBytes = ui64(cfg.GroupInfo->Type.PartSize(TLogoBlobID(Blob(opts, 0), 1))) * opts.Corpus * opts.Missing;
        Y_ENSURE(state.ObservedRecoveredPartBytes == expectedBytes);
        result["expected_recovered_part_bytes"] = expectedBytes;
        result["recovered_part_bytes"] = state.ObservedRecoveredPartBytes;
        result["recovered_part_bytes_source"] = opts.Workload == "restore"
            ? "Actual byte-verified payload size of each initially missing part type, counted once per blob regardless of main/handoff destination or redundant copies; complete physical layouts and all destinations are recorded. Interval VDisk/PDisk writes and complete-layout verification reads are separate traffic totals."
            : "Actual payload sizes from the final successful direct read of each initially missing/corrupted main part, counted once. Interval VDisk and PDisk write traffic is reported separately and may include metadata/background writes.";
        result["maintenance_verification_requests"] = state.RepairVerificationRequests;
        result["maintenance_verification_payload_bytes"] = state.RepairVerificationBytes;
    }
    if (maintenance) {
        result["completion_poll_interval_ms"] = 1000;
        result["completion_metric"] = opts.Workload == "replication" ? "all VDisk Replicated statuses plus direct byte verification of every repaired main part before cutoff" : "two successful scrub passes plus direct byte verification of every repaired main part before cutoff";
        result["fault_method"] = opts.Workload == "replication"
            ? "surviving payload seeded, full historical no-local main ingress durably applied through TEvLocalSyncData; every placement/local mask and missing main payload verified before commence; in-process fixture, no physical disk loss, restart or peer sync handshake"
            : "identified extent read errors at PDisk request boundary; all other I/O uses real files; local 100ms scrub admission service";
    }
    result["request_errors"] = state.Errors.load();
    if (state.LastResponseNs > state.FirstRequestNs && state.ActorLastUs > state.ActorFirstUs) {
        const double ratio = double(state.ActorLastUs - state.ActorFirstUs) * 1000 / (state.LastResponseNs - state.FirstRequestNs);
        result["actor_to_wall_clock_ratio"] = ratio;
        Y_ENSURE(ratio > 0.95 && ratio < 1.05, "actor monotonic clock does not track real wall time");
    }
    result["warmup_seconds"] = opts.Warmup;
    result["needs_longer_run"] = maintenance ? opts.Corpus < 100 : state.Requests < 100;
    result["driver"] = load ? "TStorageLoad ContentType=Validated" : "TEvPut/TEvGet with exact byte validation";
    if (load) {
        Y_ENSURE(loadResult.wait_for(std::chrono::seconds(125)) == std::future_status::ready, "LoadActor completion timeout");
        auto event = loadResult.get();
        const auto* finished = event->Get<TEvLoad::TEvLoadTestFinished>();
        TFileOutput(opts.Output + "/load.html").Write(finished->LastHtmlPage);
        result["load_error"] = finished->ErrorReason;
        result["load_report_present"] = bool(finished->Report);
        result["load_finished_monotonic_ns"] = state.LoadFinishedNs.load();
        result["load_stop_requested_monotonic_ns"] = state.LoadStopRequestedNs.load();
        result["measurement_end_monotonic_ns"] = end;
        Y_ENSURE(finished->Report && (finished->ErrorReason.empty() || finished->ErrorReason == "HandleStopTest"),
            "LoadActor failed: " << finished->ErrorReason);
        Y_ENSURE(state.LoadStopRequestedNs >= end && state.LoadFinishedNs >= state.LoadStopRequestedNs,
            "LoadActor finished before its explicit post-cutoff stop");
        Y_ENSURE(state.LoadFinishedNs >= end, "LoadActor stopped before the measurement window ended");
        // LoadActor may finish with requests still in flight. Validate their
        // eventual replies without adding them to the completed-window sample.
        const ui64 deadline = ClockNs() + 125ull * 1000000000;
        for (;;) {
            {
                std::lock_guard lock(state.Mutex);
                if (!state.Outstanding) break;
            }
            Y_ENSURE(ClockNs() < deadline, "LoadActor outstanding replies did not drain");
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    if (opts.Workload == "restore") {
        Y_ENSURE(CheckRestoreCompletion(cfg, opts, repairParts, nullptr, result["restore_layout_final"]),
            "final restore layout or payload verification failed; see restore_layout_final");
        TFileOutput output(opts.Output + "/restore-layouts.json");
        NJson::TJsonValue layouts(NJson::JSON_MAP);
        layouts["timed"] = result["restore_layout_timed"];
        layouts["final"] = result["restore_layout_final"];
        NJson::WriteJson(&output, &layouts, true);
    } else if (maintenance) {
        VerifyParts(cfg, opts, data);
    }
    result["actor_elapsed_seconds"] = NJson::TJsonValue(NJson::JSON_MAP);
    bool actorStatisticsAvailable = false;
    bool proxyStatisticsAvailable = false;
    for (const auto& [name, ticks] : actorEnd) {
        const auto it = actorBegin.find(name);
        const ui64 delta = ticks - (it == actorBegin.end() ? 0 : it->second);
        if (delta) {
            result["actor_elapsed_seconds"][name] = NHPTimer::GetSeconds(delta);
            actorStatisticsAvailable = true;
            proxyStatisticsAvailable |= name.Contains("BS_PROXY_PUT_ACTOR") || name.Contains("BS_PROXY_GET_ACTOR");
        }
    }
    result["actor_elapsed_statistics_available"] = actorStatisticsAvailable;
    result["dsproxy_actor_statistics_available"] = proxyStatisticsAvailable;
    {
        std::lock_guard lock(state.Mutex);
        std::sort(state.LatencyNs.begin(), state.LatencyNs.end());
        for (const ui32 percentile : {50u, 95u, 99u}) {
            if (!state.LatencyNs.empty()) result["latency_ns"][ToString(percentile)] = state.LatencyNs[(state.LatencyNs.size() - 1) * percentile / 100];
        }
        for (ui64 latency : state.LatencyNs) result["raw_latency_ns"].AppendValue(latency);
    }
    ui64 observedVWriteBytes = 0, observedPDiskWriteBytes = 0;
    for (ui32 i = 0; i < disks; ++i) {
        const auto& m = *state.Disks[i];
        NJson::TJsonValue disk(NJson::JSON_MAP);
        disk["order"] = i;
        disk["vput"] = m.VPut.load();
        disk["vmultiput"] = m.VMultiPut.load();
        disk["vget"] = m.VGet.load();
        disk["vdisk_write_payload_bytes"] = m.VWriteBytes.load();
        disk["vdisk_read_payload_bytes"] = m.VReadBytes.load();
        disk["pdisk_chunk_reads"] = m.ChunkRead.load();
        disk["pdisk_chunk_writes"] = m.ChunkWrite.load();
        disk["pdisk_chunk_read_bytes"] = m.ChunkReadBytes.load();
        disk["pdisk_chunk_write_bytes"] = m.ChunkWriteBytes.load();
        disk["pdisk_log_payload_bytes"] = m.LogBytes.load();
        disk["injected_pdisk_read_errors"] = m.InjectedReadErrors.load();
        observedVWriteBytes += m.VWriteBytes;
        observedPDiskWriteBytes += m.ChunkWriteBytes + m.LogBytes;
        result["disks"].AppendValue(std::move(disk));
    }
    result["observed_vdisk_write_payload_bytes"] = observedVWriteBytes;
    result["observed_pdisk_write_request_bytes"] = observedPDiskWriteBytes;
    result["validation_errors_total"] = state.ValidationErrors.load();
    result["unmeasured_tail_status_errors"] = state.TailStatusErrors.load();
    result["payload_validator_self_check"] = "valid fragmented ID-pattern accepted; first, chunk-boundary and tail byte corruption plus truncation rejected";
    Y_ENSURE(state.ValidationErrors == 0, "matching request failed or LoadActor payload mismatch outside measurement");
    Y_ENSURE(state.Errors == 0 && (state.Requests > 0 || maintenance), "measurement has errors or no requests");
    result["status"] = "ok";
    TFileOutput output(opts.Output + "/result.json");
    NJson::WriteJson(&output, &result, true);
    cfg.Shutdown();
    failureDiagnostics.Success();
    return 0;
}
}

int main(int argc, char** argv) {
    try {
        return Run(TOptions(argc, argv));
    } catch (const std::exception& error) {
        Cerr << error.what() << Endl;
        for (int i = 1; i + 1 < argc; ++i) {
            if (TStringBuf(argv[i]) == "--output") {
                TFsPath(argv[i + 1]).MkDirs();
                NJson::TJsonValue result(NJson::JSON_MAP);
                result["status"] = "failed";
                result["error"] = error.what();
                TFileOutput output(TString(argv[i + 1]) + "/failure.json");
                NJson::WriteJson(&output, &result, true);
                break;
            }
        }
        return 1;
    }
}
