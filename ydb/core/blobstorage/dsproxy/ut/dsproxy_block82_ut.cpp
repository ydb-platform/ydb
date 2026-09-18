#include "defs.h"
#include "dsproxy_vdisk_mock_ut.h"

#include <ydb/core/blobstorage/dsproxy/dsproxy_get_impl.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_partlayout.h>
#include <ydb/core/erasure/erasure.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace {

struct TBlock82ReadResult {
    TAutoPtr<TEvBlobStorage::TEvGetResult> Result;
    ui32 Gets = 0;
    ui32 Puts = 0;
    ui32 RepairedParts = 0;
    TSubgroupPartLayout RepairedLayout;
};

// Runs the production get and restore strategies against a persistent part mock.
// Replies are deliberately reordered; errors are delivered before successful reads.
TBlock82ReadResult ReadBlock82(TGroupMock& group, TLogoBlobID id, TStringBuf data,
        ui32 priorityReplyMask, bool restore, ui32 shift = 0, ui32 size = 0, bool phantom = false) {
    TEvBlobStorage::TEvGet request(id, shift, size, TInstant::Max(),
        NKikimrBlobStorage::FastRead, restore);
    request.PhantomCheck = phantom;
    auto queues = group.MakeGroupQueues();
    TGetImpl impl(group.GetInfo(), queues, &request, nullptr, TAccelerationParams{});
    TLogContext logCtx(NKikimrServices::BS_PROXY_GET, false);
    logCtx.SuppressLog = true;
    TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> gets;
    TDeque<std::unique_ptr<TEvBlobStorage::TEvVPut>> puts;
    impl.GenerateInitialRequests(logCtx, gets);

    TDataPartSet expected;
    TString encrypted(data);
    char* bytes = encrypted.Detach();
    Encrypt(bytes, bytes, 0, encrypted.size(), id, *group.GetInfo());
    group.GetInfo()->Type.SplitData(static_cast<TErasureType::ECrcMode>(id.CrcMode()), encrypted, expected);
    TBlock82ReadResult output;
    for (ui32 step = 0; !output.Result && step < 256; ++step) {
        if (!gets.empty()) {
            auto selected = gets.end() - 1;
            for (auto it = gets.begin(); it != gets.end(); ++it) {
                const auto disk = VDiskIDFromVDiskID((*it)->Record.GetVDiskID());
                const ui32 idx = group.GetInfo()->GetIdxInSubgroup(disk, id.Hash());
                if (priorityReplyMask >> idx & 1) {
                    selected = it;
                    break;
                }
            }
            auto get = std::move(*selected);
            gets.erase(selected);
            TEvBlobStorage::TEvVGetResult reply;
            group.OnVGet(*get, reply);
            ++output.Gets;
            impl.OnVGetResult(logCtx, reply, gets, puts, output.Result);
        } else if (!puts.empty()) {
            auto put = std::move(puts.back());
            puts.pop_back();
            const auto partId = LogoBlobIDFromLogoBlobID(put->Record.GetBlobID());
            UNIT_ASSERT_C(partId.PartId() >= 1 && partId.PartId() <= 10, partId);
            UNIT_ASSERT_EQUAL(put->GetBuffer().ConvertToString(),
                expected.Parts[partId.PartId() - 1].OwnedString.ConvertToString());
            auto status = group.OnVPut(*put);
            if (status == NKikimrProto::ALREADY) {
                status = NKikimrProto::OK;
            }
            TEvBlobStorage::TEvVPutResult reply;
            reply.MakeError(status, TString(), put->Record);
            ++output.Puts;
            if (status == NKikimrProto::OK) {
                output.RepairedParts |= ui32{1} << (partId.PartId() - 1);
                const auto disk = VDiskIDFromVDiskID(put->Record.GetVDiskID());
                output.RepairedLayout.AddItem(group.GetInfo()->GetIdxInSubgroup(disk, id.Hash()),
                    partId.PartId() - 1, group.GetInfo()->Type);
            }
            impl.OnVPutResult(logCtx, reply, gets, puts, output.Result);
        } else {
            UNIT_FAIL("Block82 Get exhausted pending requests without a result");
        }
    }
    UNIT_ASSERT_C(output.Result, "Block82 Get exceeded the bounded event budget");
    UNIT_ASSERT_VALUES_EQUAL(output.Result->ResponseSz, 1);
    if (!restore) {
        UNIT_ASSERT_VALUES_EQUAL(output.Puts, 0);
        UNIT_ASSERT(puts.empty());
    }
    return output;
}

TString Block82Data(ui32 size) {
    TString data = TString::Uninitialized(size);
    char* bytes = data.Detach();
    for (ui32 i = 0; i < size; ++i) {
        bytes[i] = char((i * 137 + i / 251) % 256);
    }
    return data;
}

void CheckBlock82Read(ui32 failureMask, bool nodeErrors, bool restore,
        TErasureType::ECrcMode crc, NKikimrProto::EReplyStatus expected,
        ui32 size = 4097, ui32 shift = 0, ui32 length = 0) {
    TActorSystemStub actorSystem;
    TGroupMock group(0, TErasureType::Erasure8Plus2Block, 12, 1, 1);
    const TString data = Block82Data(size);
    const TLogoBlobID id(1, 2, 3, 0, size, 17, 0, crc);
    group.Put(id, data);
    for (ui32 disk = 0; disk < 12; ++disk) {
        if (failureMask >> disk & 1) {
            const ui32 domain = group.DomainIdxForBlobSubgroupIdx(id, disk);
            if (nodeErrors) {
                group.SetError(domain, NKikimrProto::ERROR);
            } else {
                group.Wipe(domain);
            }
        }
    }
    auto output = ReadBlock82(group, id, data, nodeErrors ? failureMask : 0, restore, shift, length);
    UNIT_ASSERT_VALUES_EQUAL_C(output.Result->Responses[0].Status, expected,
        "mask# " << failureMask << " nodeErrors# " << nodeErrors << " restore# " << restore
        << " crc# " << ui32(crc) << " size# " << size << " shift# " << shift << " length# " << length);
    if (expected == NKikimrProto::OK) {
        UNIT_ASSERT_VALUES_EQUAL(output.Result->Status, NKikimrProto::OK);
        const TString wanted = data.substr(shift, length ? length : size - shift);
        UNIT_ASSERT_EQUAL(output.Result->Responses[0].Buffer.ConvertToString(), wanted);
        if (restore && (failureMask & 0x3ff)) {
            UNIT_ASSERT(output.Puts);
            UNIT_ASSERT(output.RepairedParts);
            // Repaired payload is stored in the mock and remains readable by a new request.
            const auto again = ReadBlock82(group, id, data, nodeErrors ? failureMask : 0, false);
            UNIT_ASSERT_VALUES_EQUAL(again.Result->Responses[0].Status, NKikimrProto::OK);
            UNIT_ASSERT_EQUAL(again.Result->Responses[0].Buffer.ConvertToString(), data);
        }
    } else {
        UNIT_ASSERT(output.Result->Responses[0].Buffer.empty());
    }
}

void Block82FailureCorpus(bool restore, bool nodeErrors) {
    for (auto crc : {TErasureType::CrcModeNone, TErasureType::CrcModeWholePart}) {
        CheckBlock82Read(0, nodeErrors, restore, crc, NKikimrProto::OK);
        for (ui32 first = 0; first < 12; ++first) {
            CheckBlock82Read(ui32{1} << first, nodeErrors, restore, crc, NKikimrProto::OK);
            for (ui32 second = 0; second < first; ++second) {
                CheckBlock82Read((ui32{1} << first) | (ui32{1} << second),
                    nodeErrors, restore, crc, NKikimrProto::OK);
            }
        }
    }
}

} // namespace

Y_UNIT_TEST_SUITE(DSProxyBlock82) {
    Y_UNIT_TEST(Block82AllSingleDoubleMissing) { Block82FailureCorpus(false, false); }
    Y_UNIT_TEST(Block82AllSingleDoubleErrors) { Block82FailureCorpus(false, true); }
    Y_UNIT_TEST(Block82AllSingleDoubleRepairMissing) { Block82FailureCorpus(true, false); }
    Y_UNIT_TEST(Block82AllSingleDoubleRepairErrors) { Block82FailureCorpus(true, true); }

    Y_UNIT_TEST(Block82TripleFailureClassification) {
        for (auto crc : {TErasureType::CrcModeNone, TErasureType::CrcModeWholePart}) {
            for (ui32 mask : {0x007u, 0x103u, 0x301u}) {
                CheckBlock82Read(mask, false, false, crc, NKikimrProto::NODATA);
                CheckBlock82Read(mask, false, true, crc, NKikimrProto::NODATA);
                CheckBlock82Read(mask, true, false, crc, NKikimrProto::ERROR);
            }
            for (ui32 mask : {0x403u, 0x501u, 0x700u, 0xc01u, 0xd00u}) {
                CheckBlock82Read(mask, false, false, crc, NKikimrProto::OK);
                CheckBlock82Read(mask, true, true, crc, NKikimrProto::ERROR);
            }
        }
    }

    Y_UNIT_TEST(Block82MinIOFragmentBoundaries) {
        for (auto crc : {TErasureType::CrcModeNone, TErasureType::CrcModeWholePart}) {
            for (ui32 size : {1u, 31u, 32u, 255u, 256u, 257u, 4097u, 1048579u}) {
                for (ui32 mask : {0u, 1u, 3u, 0x101u, 0x300u}) {
                    const ui32 shift = size > 256 ? 255 : 0;
                    const ui32 length = Min<ui32>(65, size - shift);
                    CheckBlock82Read(mask, false, false, crc, NKikimrProto::OK, size, shift, length);
                }
            }
        }
    }

    Y_UNIT_TEST(Block82DenseHandoffReadAndPhantom) {
        for (auto crc : {TErasureType::CrcModeNone, TErasureType::CrcModeWholePart}) {
            TActorSystemStub actorSystem;
            TGroupMock group(0, TErasureType::Erasure8Plus2Block, 12, 1, 1);
            const auto info = group.GetInfo();
            const TString data = Block82Data(4097);
            const TLogoBlobID id(1, 2, 3, 0, data.size(), 17, 0, crc);
            TString encrypted = data;
            char* bytes = encrypted.Detach();
            Encrypt(bytes, bytes, 0, encrypted.size(), id, *info);
            TDataPartSet parts;
            info->Type.SplitData(crc, encrypted, parts);
            TSubgroupPartLayout layout;
            auto write = [&](ui32 disk, ui32 part) {
                TEvBlobStorage::TEvVPut request(TLogoBlobID(id, part + 1), parts.Parts[part].OwnedString,
                    info->GetVDiskInSubgroup(disk, id.Hash()), true, nullptr, TInstant::Max(),
                    NKikimrBlobStorage::TabletLog);
                UNIT_ASSERT_VALUES_EQUAL(group.OnVPut(request), NKikimrProto::OK);
                layout.AddItem(disk, part, info->Type);
            };
            for (ui32 part = 0; part < 8; ++part) {
                write(part < 3 ? 10 : part, part);
            }
            ui32 distinct = 0;
            for (ui32 part = 0; part < 10; ++part) {
                distinct += bool(layout.GetDisksWithPart(part));
            }
            UNIT_ASSERT_VALUES_EQUAL(distinct, 8);
            UNIT_ASSERT_VALUES_EQUAL(layout.CountEffectiveReplicas(info->Type), 6);

            // Deliver the readable parts first: ordinary MinIO Get can finish
            // from their bytes without establishing an independent placement.
            constexpr ui32 readableDisks = 0x4f8; // mains 3..7 and handoff 10
            auto read = ReadBlock82(group, id, data, readableDisks, false);
            UNIT_ASSERT_VALUES_EQUAL(read.Result->Responses[0].Status, NKikimrProto::OK);
            UNIT_ASSERT_EQUAL(read.Result->Responses[0].Buffer.ConvertToString(), data);
            // PhantomCheck and MustRestoreFirst use the bold strategy, whose
            // placement decision counts independent replicas, not just parts.
            auto phantom = ReadBlock82(group, id, data, readableDisks, false, 0, 0, true);
            UNIT_ASSERT_VALUES_EQUAL(phantom.Result->Responses[0].Status, NKikimrProto::NODATA);
            UNIT_ASSERT(phantom.Result->Responses[0].LooksLikePhantom.value_or(false));
            auto nonquorate = ReadBlock82(group, id, data, readableDisks, true);
            UNIT_ASSERT_VALUES_EQUAL(nonquorate.Result->Responses[0].Status, NKikimrProto::NODATA);
            UNIT_ASSERT_VALUES_EQUAL(nonquorate.Puts, 0);

            // Two extra main copies make eight independent replicas while
            // retaining the dense handoff; repair must persist parity parts 9/10
            // and establish ten independent replicas. Part 3 may stay on handoff.
            write(0, 0);
            write(1, 1);
            UNIT_ASSERT_VALUES_EQUAL(layout.CountEffectiveReplicas(info->Type), 8);
            auto repaired = ReadBlock82(group, id, data, readableDisks | 3, true);
            UNIT_ASSERT_VALUES_EQUAL(repaired.Result->Responses[0].Status, NKikimrProto::OK);
            UNIT_ASSERT_EQUAL(repaired.Result->Responses[0].Buffer.ConvertToString(), data);
            UNIT_ASSERT_C((repaired.RepairedParts & 0x300) == 0x300, repaired.RepairedParts);
            layout.Merge(repaired.RepairedLayout, info->Type);
            UNIT_ASSERT_VALUES_EQUAL(layout.CountEffectiveReplicas(info->Type), 10);
            auto again = ReadBlock82(group, id, data, 0, false);
            UNIT_ASSERT_VALUES_EQUAL(again.Result->Responses[0].Status, NKikimrProto::OK);
            UNIT_ASSERT_EQUAL(again.Result->Responses[0].Buffer.ConvertToString(), data);
        }
    }
}
} // namespace NKikimr
