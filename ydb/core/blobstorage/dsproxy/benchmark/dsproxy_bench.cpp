#include <ydb/core/blobstorage/dsproxy/dsproxy_get_impl.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy_put_impl.h>
#include <ydb/core/blobstorage/dsproxy/ut/dsproxy_vdisk_mock_ut.h>

#include <library/cpp/json/json_value.h>
#include <util/generic/yexception.h>
#include <util/string/cast.h>

#include <bit>
#include <ctime>

namespace NKikimr {
namespace {

ui64 Nanos(clockid_t clock) {
    timespec time;
    Y_ENSURE(clock_gettime(clock, &time) == 0);
    return ui64(time.tv_sec) * 1'000'000'000 + time.tv_nsec;
}

struct TTraffic {
    ui64 Gets = 0;
    ui64 Puts = 0;
    ui64 ReadBytes = 0;
    ui64 WriteBytes = 0;
    ui64 RepairBytes = 0;

    void Add(const TTraffic& other) {
        Gets += other.Gets;
        Puts += other.Puts;
        ReadBytes += other.ReadBytes;
        WriteBytes += other.WriteBytes;
        RepairBytes += other.RepairBytes;
    }
};

struct TResult {
    TTraffic Traffic;
    TAutoPtr<TEvBlobStorage::TEvGetResult> Get;
    TVector<std::pair<ui32, TRope>> WrittenParts;
};

// Persistent in-memory VDisks are shared with correctness tests. All routing,
// request generation, restoration and encoding use the production components.
class TFixture {
    TActorSystemStub ActorSystem;
    TBlobStorageGroupType Type;
    TGroupMock Group;
    TIntrusivePtr<TGroupQueues> Queues;
    TString Data;
    TLogoBlobID Id;
    TDataPartSet Expected;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    TIntrusivePtr<TDsProxyNodeMon> NodeMon;
    TIntrusivePtr<TBlobStorageGroupProxyMon> Mon;
    TLogContext LogCtx;

    void Write(TEvBlobStorage::TEvVPut& request, TResult& result, bool repair,
            TEvBlobStorage::TEvVPutResult& reply) {
        const auto id = LogoBlobIDFromLogoBlobID(request.Record.GetBlobID());
        const TRope buffer = request.GetBuffer();
        result.WrittenParts.emplace_back(id.PartId() - 1, buffer);
        ++result.Traffic.Puts;
        result.Traffic.WriteBytes += buffer.size();
        if (repair) {
            result.Traffic.RepairBytes += buffer.size();
        }
        const auto status = Group.OnVPut(request);
        Y_ENSURE(status == NKikimrProto::OK || status == NKikimrProto::ALREADY);
        reply.MakeError(NKikimrProto::OK, TString(), request.Record);
    }

public:
    TFixture(TErasureType::EErasureSpecies species, ui32 size, TErasureType::ECrcMode crc)
        : Type(species)
        , Group(0, species, Type.BlobSubgroupSize(), 1, 1)
        , Queues(Group.MakeGroupQueues())
        , Data(TString::Uninitialized(size))
        , Id(1, 2, 3, 0, size, 17, 0, crc)
        , Counters(new ::NMonitoring::TDynamicCounters())
        , NodeMon(new TDsProxyNodeMon(Counters, true))
        , Mon(new TBlobStorageGroupProxyMon(Counters, Counters, Counters, Group.GetInfo(), NodeMon, false))
        , LogCtx(NKikimrServices::BS_PROXY_GET, false)
    {
        LogCtx.SuppressLog = true;
        LogCtx.LogAcc.IsLogEnabled = false;
        ui64 seed = 0x82422026;
        char* bytes = Data.Detach();
        for (ui32 i = 0; i < size; ++i) {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            bytes[i] = char(seed);
        }
        TString encrypted = Data;
        char* buffer = encrypted.Detach();
        Encrypt(buffer, buffer, 0, size, Id, *Group.GetInfo());
        Type.SplitData(crc, encrypted, Expected);
    }

    void Prepare(bool put, ui32 failedMask) {
        Group.Wipe();
        if (!put) {
            Group.Put(Id, Data);
            for (ui32 disk = 0; disk < Type.BlobSubgroupSize(); ++disk) {
                if (failedMask >> disk & 1) {
                    Group.Wipe(Group.DomainIdxForBlobSubgroupIdx(Id, disk));
                }
            }
        }
    }

    TResult Put() {
        TResult result;
        TString encrypted = Data;
        char* buffer = encrypted.Detach();
        Encrypt(buffer, buffer, 0, encrypted.size(), Id, *Group.GetInfo());
        TBatchedVec<TStackVec<TRope, TypicalPartsInBlob>> parts(1);
        parts[0].resize(Type.TotalPartCount());
        ErasureSplit(static_cast<TErasureType::ECrcMode>(Id.CrcMode()), Type, TRope(encrypted), parts[0],
            nullptr, GetDefaultRcBufAllocator());
        TEvBlobStorage::TEvPut request(Id, Data, TInstant::Max(), NKikimrBlobStorage::TabletLog);
        TPutImpl impl(Group.GetInfo(), Queues, &request, Mon, false, TActorId(), 0,
            NWilson::TTraceId(), TAccelerationParams{});
        TPutImpl::TPutResultVec replies;
        impl.GenerateInitialRequests(LogCtx, parts);
        impl.Step(LogCtx, replies, {&Group.GetInfo()->GetTopology()}, false);
        auto requests = impl.GeneratePutRequests();
        for (ui32 step = 0; replies.empty() && step < 256; ++step) {
            Y_ENSURE(!requests.empty(), "Put exhausted pending requests");
            auto request = std::move(requests.back());
            requests.pop_back();
            auto& vput = std::get<0>(request);
            TEvBlobStorage::TEvVPutResult reply;
            Write(*vput, result, false, reply);
            impl.ProcessResponse(reply);
            impl.Step(LogCtx, replies, {&Group.GetInfo()->GetTopology()}, false);
            auto next = impl.GeneratePutRequests();
            std::move(next.begin(), next.end(), std::back_inserter(requests));
        }
        Y_ENSURE(replies.size() == 1 && replies.front().second->Status == NKikimrProto::OK);
        Y_ENSURE(replies.front().second->Id == Id);
        Y_ENSURE(requests.empty(), "healthy Put left outstanding requests");
        return result;
    }

    TResult Get(bool restore) {
        TResult result;
        TEvBlobStorage::TEvGet request(Id, 0, 0, TInstant::Max(), NKikimrBlobStorage::FastRead, restore);
        TGetImpl impl(Group.GetInfo(), Queues, &request, nullptr, TAccelerationParams{});
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> gets;
        TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> puts;
        impl.GenerateInitialRequests(LogCtx, gets);
        for (ui32 step = 0; !result.Get && step < 256; ++step) {
            if (!gets.empty()) {
                auto get = std::move(gets.back());
                gets.pop_back();
                TEvBlobStorage::TEvVGetResult reply;
                Group.OnVGet(*get, reply);
                ++result.Traffic.Gets;
                for (const auto& item : reply.Record.GetResult()) {
                    result.Traffic.ReadBytes += reply.GetBlobSize(item);
                }
                impl.OnVGetResult(LogCtx, reply, gets, puts, result.Get);
            } else {
                Y_ENSURE(!puts.empty(), "Get exhausted pending requests");
                auto put = std::move(puts.back());
                puts.pop_back();
                TEvBlobStorage::TEvVPutResult reply;
                Write(*put, result, true, reply);
                impl.OnVPutResult(LogCtx, reply, gets, puts, result.Get);
            }
        }
        Y_ENSURE(result.Get && result.Get->Status == NKikimrProto::OK);
        Y_ENSURE(result.Get->ResponseSz == 1 && result.Get->Responses[0].Status == NKikimrProto::OK);
        Y_ENSURE(restore || (!result.Traffic.Puts && puts.empty()));
        // Requests have already been emitted by the strategy. Drain their mock
        // I/O even if their reply is no longer needed after client completion,
        // so traffic reports include every issued request and payload.
        for (auto& get : gets) {
            TEvBlobStorage::TEvVGetResult reply;
            Group.OnVGet(*get, reply);
            ++result.Traffic.Gets;
            for (const auto& item : reply.Record.GetResult()) {
                result.Traffic.ReadBytes += reply.GetBlobSize(item);
            }
        }
        for (auto& put : puts) {
            TEvBlobStorage::TEvVPutResult reply;
            Write(*put, result, true, reply);
        }
        return result;
    }

    void Validate(const TResult& result, bool put, bool restore, ui32 failedMask) {
        for (const auto& [part, bytes] : result.WrittenParts) {
            Y_ENSURE(part < Type.TotalPartCount());
            Y_ENSURE(bytes.ConvertToString() == Expected.Parts[part].OwnedString.ConvertToString(),
                "encoded or restored part bytes differ");
        }
        if (!put) {
            Y_ENSURE(result.Get->Responses[0].Id == Id);
            Y_ENSURE(result.Get->Responses[0].Buffer.ConvertToString() == Data, "Get payload differs");
        }
        if (restore && (failedMask & ((ui32{1} << Type.TotalPartCount()) - 1))) {
            Y_ENSURE(result.Traffic.Puts && result.Traffic.RepairBytes, "restore did not write repaired parts");
        }
        if (put || restore) {
            // A new request sees the bytes stored by Put/repair. Its work is untimed.
            auto reread = Get(false);
            Y_ENSURE(reread.Get->Responses[0].Buffer.ConvertToString() == Data);
        }
    }
};

ui32 ParseMask(TStringBuf text, const TBlobStorageGroupType& type) {
    if (text == "D") return 1;
    if (text == "DD") return 3;
    if (text == "DP") return 1 | (ui32{1} << type.DataParts());
    if (text == "PP") return 3 << type.DataParts();
    return FromString<ui32>(text);
}

void Run(TErasureType::EErasureSpecies species, ui32 size, TStringBuf operation,
        TStringBuf maskText, ui64 iterations, TErasureType::ECrcMode crc) {
    const TBlobStorageGroupType type(species);
    const ui32 mask = ParseMask(maskText, type);
    const bool put = operation == "put", restore = operation == "restore";
    Y_ENSURE(put || restore || operation == "get", "operation must be put/get/restore");
    Y_ENSURE(size && size <= 10 * 1024 * 1024 && iterations && iterations <= 1000000);
    Y_ENSURE(!(mask >> type.BlobSubgroupSize()) && std::popcount(mask) <= 2);
    Y_ENSURE(!put || !mask, "Put cell is healthy; use get/restore for degraded cells");
    TFixture fixture(species, size, crc);
    // Validate before measurement and warm the exact cell. Setup and validation
    // are outside every timed region, including every measured iteration.
    for (ui32 i = 0; i < 3; ++i) {
        fixture.Prepare(put, mask);
        auto result = put ? fixture.Put() : fixture.Get(restore);
        fixture.Validate(result, put, restore, mask);
    }
    TTraffic total;
    ui64 wall = 0, cpu = 0;
    for (ui64 i = 0; i < iterations; ++i) {
        fixture.Prepare(put, mask);
        const ui64 cpuBegin = Nanos(CLOCK_PROCESS_CPUTIME_ID);
        const ui64 wallBegin = Nanos(CLOCK_MONOTONIC_RAW);
        auto result = put ? fixture.Put() : fixture.Get(restore);
        wall += Nanos(CLOCK_MONOTONIC_RAW) - wallBegin;
        cpu += Nanos(CLOCK_PROCESS_CPUTIME_ID) - cpuBegin;
        fixture.Validate(result, put, restore, mask);
        total.Add(result.Traffic);
    }
    const double logicalBytes = double(size) * iterations;
    NJson::TJsonValue report;
    report["harness"] = "production_dsproxy_impl_with_in_memory_vdisks";
    report["timing_scope"] = "impl_codec_and_all_issued_mock_io";
    report["species"] = type.ToString();
    report["operation"] = TString(operation);
    report["failure_kind"] = "NODATA";
    report["failed_subgroup_mask"] = mask;
    report["size"] = size;
    report["crc"] = crc == TErasureType::CrcModeNone ? "none" : "whole_part";
    report["seed"] = ui64(0x82422026);
    report["operations"] = iterations;
    report["warmup_operations"] = 3;
    report["wall_ns"] = wall;
    report["process_cpu_ns"] = cpu;
    report["wall_ns_per_logical_byte"] = wall / logicalBytes;
    report["process_cpu_ns_per_logical_byte"] = cpu / logicalBytes;
    report["logical_bytes_per_wall_second"] = logicalBytes * 1e9 / wall;
    report["vget_requests"] = total.Gets;
    report["vput_requests"] = total.Puts;
    report["vdisk_read_bytes"] = total.ReadBytes;
    report["vdisk_write_bytes"] = total.WriteBytes;
    report["repair_write_bytes"] = total.RepairBytes;
    report["validated_operations"] = iterations + 3;
    Cout << report << Endl;
}

} // namespace
} // namespace NKikimr

int main(int argc, char** argv) {
    try {
        Y_ENSURE(argc >= 6 && argc <= 7 && (TStringBuf(argv[1]) == "42" || TStringBuf(argv[1]) == "82"),
            "usage: dsproxy_bench 42|82 bytes put|get|restore 0|D|DD|DP|PP|decimal_mask iterations [none|whole]");
        const TStringBuf crc = argc == 7 ? argv[6] : "none";
        Y_ENSURE(crc == "none" || crc == "whole");
        NKikimr::Run(TStringBuf(argv[1]) == "82" ? NKikimr::TErasureType::Erasure8Plus2Block
                : NKikimr::TErasureType::Erasure4Plus2Block,
            FromString<ui32>(argv[2]), argv[3], argv[4], FromString<ui64>(argv[5]),
            crc == "none" ? NKikimr::TErasureType::CrcModeNone : NKikimr::TErasureType::CrcModeWholePart);
        return 0;
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
