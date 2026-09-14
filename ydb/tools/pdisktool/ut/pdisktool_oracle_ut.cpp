#include <ydb/core/base/appdata.h>
#include <ydb/core/blobstorage/crypto/default.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_config.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_tools.h>
#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/pdisk_io/aio.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/tools/pdisktool/lib/blobs.h>
#include <ydb/tools/pdisktool/lib/commands.h>
#include <ydb/tools/pdisktool/lib/format.h>
#include <ydb/tools/pdisktool/lib/hull.h>
#include <ydb/tools/pdisktool/lib/sector.h>
#include <ydb/tools/pdisktool/lib/session.h>

#include <library/cpp/testing/unittest/registar.h>

#include <cstring>
#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/system/fs.h>
#include <ydb/tools/pdisktool/lib/keys.h>

using namespace NKikimr;
using namespace NKikimr::NPDisk;
using namespace NKikimr::NPDiskTool;

namespace NKikimr {
namespace NPDisk {
extern const ui64 YdbDefaultPDiskSequence = 0x7e5700007e570000;
}
}

namespace {

constexpr ui32 ChunkSize = 32 << 20;
constexpr ui64 DiskSize = ui64(ChunkSize) * 250;
constexpr ui64 MainKeyValue = NPDisk::YdbDefaultPDiskSequence;

TIntrusivePtr<NPDisk::TSectorMap> FormatMap(ui64 guid) {
    auto map = MakeIntrusive<NPDisk::TSectorMap>(DiskSize);
    TFormatOptions options;
    options.SectorMap = map;
    options.EnableSmallDiskOptimization = true;
    FormatPDisk("", DiskSize, 4096, ChunkSize, guid,
        NPDisk::TKey(1), NPDisk::TKey(2), NPDisk::TKey(3), MainKeyValue, "pdisktool-ut", options);
    return map;
}

TSessionOptions DefaultOpts() {
    TSessionOptions opts;
    opts.MainKey = TMainKey{.Keys = {MainKeyValue}, .IsInitialized = true};
    opts.TryLock = false;
    return opts;
}

TPDiskSession OpenTool(TIntrusivePtr<NPDisk::TSectorMap> map) {
    TPDiskSession session;
    UNIT_ASSERT_C(session.OpenSectorMap(std::move(map), DefaultOpts()), "pdisktool failed to open formatted SectorMap");
    UNIT_ASSERT(session.FormatResult.Ok);
    return session;
}

struct TYard {
    THolder<TTestActorRuntime> Runtime;
    std::shared_ptr<NPDisk::IIoContextFactory> IoContext;
    TIntrusivePtr<NPDisk::TSectorMap> Map;
    TActorId PDiskActor;
    TActorId Edge;
    ui64 Guid = 1;

    explicit TYard(ui64 guid = 1)
        : Guid(guid)
    {
        Map = FormatMap(guid);
        Runtime.Reset(new TTestActorRuntime(1, 1, true));
        auto app = MakeHolder<TAppData>(0, 0, 0, 0, TMap<TString, ui32>(), nullptr, nullptr, nullptr, nullptr);
        IoContext = std::make_shared<NPDisk::TIoContextFactoryOSS>();
        app->IoContextFactory = IoContext.get();
        Runtime->SetLogBackend(NActors::CreateNullBackend());
        Runtime->Initialize(TTestActorRuntime::TEgg{app.Release(), nullptr, {}, {}, {}});
        Runtime->SetLogPriority(NKikimrServices::BS_PDISK, NActors::NLog::PRI_ERROR);
        Edge = Runtime->AllocateEdgeActor();

        TIntrusivePtr<TPDiskConfig> cfg = new TPDiskConfig(TString(), guid, 1, 0);
        cfg->GetDriveDataSwitch = NKikimrBlobStorage::TPDiskConfig::DoNotTouch;
        cfg->WriteCacheSwitch = NKikimrBlobStorage::TPDiskConfig::DoNotTouch;
        cfg->ChunkSize = ChunkSize;
        cfg->SectorMap = Map;
        cfg->EnableFormatAndMetadataEncryption = true;
        cfg->FeatureFlags.SetEnableSmallDiskOptimization(true);
        cfg->FeatureFlags.SetEnablePDiskDataEncryption(false);
        cfg->FeatureFlags.SetEnablePDiskLogForSmallDisks(true);
        TMainKey mainKey{.Keys = {MainKeyValue}, .IsInitialized = true};
        auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        PDiskActor = Runtime->Register(CreatePDisk(cfg.Get(), mainKey, counters));
    }

    template <typename TRes>
    THolder<TRes> Call(IEventBase* ev) {
        Runtime->Send(new IEventHandle(PDiskActor, Edge, ev));
        auto res = Runtime->GrabEdgeEvent<TRes>();
        UNIT_ASSERT(res);
        UNIT_ASSERT_VALUES_EQUAL_C(res->Status, NKikimrProto::OK, res->ToString());
        return res;
    }

    void Stop() {
        Call<TEvYardControlResult>(new TEvYardControl(TEvYardControl::PDiskStop, nullptr));
        Runtime->Send(new IEventHandle(PDiskActor, Edge, new NActors::TEvents::TEvPoisonPill));
    }
};

bool HasMessage(const TIssueLog& issues, const TString& needle, const TString& location = TString()) {
    for (const auto& i : issues.Items) {
        if (i.Message.find(needle) != TString::npos && (!location || i.Location == location)) {
            return true;
        }
    }
    return false;
}

// Rewrite one sector with a patched payload, restoring the encryption and the sector hash the tool
// verifies. A plain byte flip is rejected before the parser under test ever sees the bytes.
template <class TPatch>
void RewriteSector(NPDisk::TSectorMap& map, const TDiskFormat& format, ui64 hashOffset, ui64 writeOffset,
        ui64 magic, const TKey& key, TPatch patch)
{
    TVector<ui8> raw(format.SectorSize);
    UNIT_ASSERT(map.Read(raw.data(), format.SectorSize, writeOffset));
    auto* footer = reinterpret_cast<TDataSectorFooter*>(
        raw.data() + format.SectorSize - sizeof(TDataSectorFooter));
    const ui64 nonce = footer->Nonce;
    const bool encrypted = footer->IsEncrypted();
    const ui32 body = format.SectorSize - sizeof(TDataSectorFooter);
    DecryptInPlace(raw.data(), body, key, nonce, encrypted);
    patch(raw.data());
    DecryptInPlace(raw.data(), body, key, nonce, encrypted);
    TPDiskHashCalculator hasher;
    footer->Hash = hasher.T1ha0HashSector<TT1ha0NoAvxHasher>(hashOffset, magic, raw.data(), format.SectorSize);
    UNIT_ASSERT(map.Write(raw.data(), format.SectorSize, writeOffset));
}

// Offset of the first record page in a decrypted log sector, walking headers the way the scanner does.
ui32 FindFirstLogPage(const ui8* payload, ui32 payloadSize) {
    ui64 offset = 0;
    while (offset + sizeof(TFirstLogPageHeader) <= payloadSize) {
        const auto* h = reinterpret_cast<const TLogPageHeader*>(payload + offset);
        if (h->Flags & NPDisk::LogPageTerminator) {
            break;
        }
        if (h->Flags & NPDisk::LogPageNonceJump2) {
            offset += sizeof(TNonceJumpLogPageHeader2);
        } else if (h->Flags & NPDisk::LogPageNonceJump1) {
            offset += sizeof(TNonceJumpLogPageHeader1);
        } else if (h->Flags & NPDisk::LogPageFirst) {
            return offset;
        } else {
            offset += sizeof(TLogPageHeader) + h->Size;
        }
    }
    return Max<ui32>();
}

TString MakeLogoBlobOptPayload(const TLogoBlobID& id, const TString& data) {
    TString out = TString::Uninitialized(sizeof(TLogoBlobID) + data.size());
    memcpy(out.Detach(), &id, sizeof(TLogoBlobID));
    memcpy(out.Detach() + sizeof(TLogoBlobID), data.data(), data.size());
    return out;
}

} // namespace

Y_UNIT_TEST_SUITE(TPDiskToolOracle) {
    Y_UNIT_TEST(FormatAndSysLogMatchWriter) {
        const ui64 guid = 0xABCDEFull;
        auto map = FormatMap(guid);
        auto session = OpenTool(map);
        UNIT_ASSERT_VALUES_EQUAL(session.Format.Guid, guid);
        UNIT_ASSERT_VALUES_EQUAL(session.Format.SectorSize, 4096u);
        UNIT_ASSERT(session.Format.ChunkSize > 0);
        UNIT_ASSERT_VALUES_EQUAL(session.Format.ChunkSize % session.Format.SectorSize, 0u);
        UNIT_ASSERT_VALUES_EQUAL(session.Format.Version, ui64(PDISK_FORMAT_VERSION));
        UNIT_ASSERT(session.SysLogRaw.Ok);
        UNIT_ASSERT(session.State.Record.LogHeadChunkIdx != 0);
        ui32 formatOk = 0;
        for (const auto& r : session.FormatResult.Replicas) {
            formatOk += r.HashOk;
        }
        UNIT_ASSERT_VALUES_EQUAL(formatOk, 3u);
        bool haveSystem = false;
        for (const auto& c : session.State.Chunks) {
            if (c.OwnerId == EOwner::OwnerSystem || c.OwnerId == EOwner::OwnerSystemLog) {
                haveSystem = true;
                break;
            }
        }
        UNIT_ASSERT(haveSystem);
    }

    Y_UNIT_TEST(OwnersChunksAndCommits) {
        TYard yard;
        const TVDiskID vdisk(1, 1, 0, 0, 0);
        auto init = yard.Call<TEvYardInitResult>(new TEvYardInit(2, vdisk, yard.Guid));
        const TOwner owner = init->PDiskParams->Owner;
        const TOwnerRound round = init->PDiskParams->OwnerRound;

        auto reserved = yard.Call<TEvChunkReserveResult>(new TEvChunkReserve(owner, round, 1));
        UNIT_ASSERT_VALUES_EQUAL(reserved->ChunkIds.size(), 1u);
        const TChunkIdx chunk = reserved->ChunkIds[0];

        NPDisk::TCommitRecord commit;
        commit.CommitChunks.push_back(chunk);
        commit.IsStartingPoint = true;
        yard.Call<TEvLogResult>(new TEvLog(owner, round, TLogSignature::SignatureHullCutLog, commit,
            TRcBuf(TString("entry")), TLsnSeg(1, 1), nullptr));

        TString payload(4096, 'Z');
        yard.Call<TEvChunkWriteResult>(new TEvChunkWrite(owner, round, chunk, 0,
            new TEvChunkWrite::TAlignedParts(TString(payload)), nullptr, true, 1));

        NPDisk::TCommitRecord del;
        del.DeleteChunks.push_back(chunk);
        yard.Call<TEvLogResult>(new TEvLog(owner, round, TLogSignature::First, del,
            TRcBuf(TString()), TLsnSeg(2, 2), nullptr));

        auto reserved2 = yard.Call<TEvChunkReserveResult>(new TEvChunkReserve(owner, round, 1));
        const TChunkIdx chunk2 = reserved2->ChunkIds[0];
        NPDisk::TCommitRecord commit2;
        commit2.CommitChunks.push_back(chunk2);
        yard.Call<TEvLogResult>(new TEvLog(owner, round, TLogSignature::First, commit2,
            TRcBuf(TString()), TLsnSeg(3, 3), nullptr));

        yard.Stop();

        auto session = OpenTool(yard.Map);
        UNIT_ASSERT(owner < session.State.Owners.size());
        UNIT_ASSERT_VALUES_EQUAL(session.State.Owners[owner].VDiskId.GroupID.GetRawId(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(ui32(session.State.Owners[owner].VDiskId.FailRealm), 0u);

        UNIT_ASSERT(chunk < session.State.Chunks.size());
        UNIT_ASSERT_VALUES_EQUAL(ui32(session.State.Chunks[chunk].OwnerId), ui32(EOwner::OwnerUnallocated));
        UNIT_ASSERT(chunk2 < session.State.Chunks.size());
        UNIT_ASSERT_VALUES_EQUAL(ui32(session.State.Chunks[chunk2].OwnerId), ui32(owner));
        UNIT_ASSERT_VALUES_EQUAL(session.State.Chunks[chunk2].CommitState, TChunkState::DATA_COMMITTED);

        bool haveCutLog = false;
        for (const auto& [sig, rec] : session.State.Owners[owner].StartingPoints) {
            if (sig == TLogSignature::SignatureHullCutLog) {
                haveCutLog = true;
                UNIT_ASSERT_VALUES_EQUAL(rec.first, 1u);
            }
        }
        UNIT_ASSERT(haveCutLog);

        TIssueLog issues;
        ui64 bytes = 0;
        ui32 gaps = 0;
        TTempDir tmp;
        TString path = TString(tmp()) + "/chunk.bin";
        UNIT_ASSERT(WriteChunkToFile(*session.Device, session.Format, session.State, chunk2, path, false,
            issues, bytes, gaps));
        UNIT_ASSERT(bytes > 0);
    }

    Y_UNIT_TEST(LogBlobAndUnknownSignature) {
        TYard yard;
        const TVDiskID vdisk(7, 1, 0, 0, 1);
        auto init = yard.Call<TEvYardInitResult>(new TEvYardInit(2, vdisk, yard.Guid));
        const TOwner owner = init->PDiskParams->Owner;
        const TOwnerRound round = init->PDiskParams->OwnerRound;

        TLogoBlobID id(1001ull, 1, 1, 0, 16, 0, 1);
        TString data(16, 'B');
        TString rec = MakeLogoBlobOptPayload(id, data);
        yard.Call<TEvLogResult>(new TEvLog(owner, round, TLogSignature::SignatureLogoBlobOpt,
            TRcBuf(rec), TLsnSeg(1, 1), nullptr));

        TString unknown(8, 'U');
        yard.Call<TEvLogResult>(new TEvLog(owner, round, TLogSignature(30),
            TRcBuf(unknown), TLsnSeg(2, 2), nullptr));

        NKikimrBlobStorage::TEvVBlock block;
        block.SetTabletId(42);
        block.SetGeneration(3);
        TString blockBytes;
        UNIT_ASSERT(block.SerializeToString(&blockBytes));
        yard.Call<TEvLogResult>(new TEvLog(owner, round, TLogSignature::SignatureBlock,
            TRcBuf(blockBytes), TLsnSeg(3, 3), nullptr));

        yard.Stop();

        auto session = OpenTool(yard.Map);
        bool sawUnknown = false;
        bool sawOpt = false;
        for (const auto& r : session.Log.Records) {
            if (r.OwnerId == owner && r.Signature.GetUnmasked() == 30) {
                sawUnknown = true;
            }
            if (r.OwnerId == owner && r.Signature.GetUnmasked() == TLogSignature::SignatureLogoBlobOpt) {
                sawOpt = true;
            }
        }
        UNIT_ASSERT(sawUnknown);
        UNIT_ASSERT(sawOpt);

        TIssueLog issues;
        auto snap = ReconstructHull(*session.Device, session.Format, session.State, session.Log, owner,
            TErasureType::ErasureNone, issues);
        NKikimr::NPdiskTool::TBlobsResult blobs;
        ListBlobs(snap, TListFilter{}, issues, blobs);
        UNIT_ASSERT_VALUES_EQUAL(blobs.BlobsSize(), 1u);
        const auto& listed = blobs.GetBlobs(0);
        UNIT_ASSERT_VALUES_EQUAL(listed.GetLogoBlobId(), TLogoBlobID(id, 0).ToString());
        UNIT_ASSERT_VALUES_EQUAL(listed.PartsSize(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(listed.GetParts(0).GetPartId(), id.PartId());
        UNIT_ASSERT_VALUES_EQUAL(listed.GetParts(0).GetSize(), data.size());
        UNIT_ASSERT_VALUES_EQUAL(listed.GetParts(0).GetBlobType(), TBlobType::TypeToStr(TBlobType::MemBlob));
        UNIT_ASSERT_VALUES_EQUAL(listed.GetParts(0).GetCopies(), 1u);

        NKikimr::NPdiskTool::TBlocksResult blocks;
        ListBlocks(snap, TListFilter{}, blocks);
        UNIT_ASSERT_VALUES_EQUAL(blocks.BlocksSize(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(blocks.GetBlocks(0).GetTabletId(), 42u);
        UNIT_ASSERT_VALUES_EQUAL(blocks.GetBlocks(0).GetBlockedGeneration(), 3u);

        TTempDir tmp;
        TExportStats stats;
        UNIT_ASSERT(ExportBlobParts(*session.Device, session.Format, session.State, snap,
            TLogoBlobID(1001ull, 1, 1, 0, 16, 0), {}, TListFilter{}, TString(tmp()), issues, stats));
        UNIT_ASSERT_VALUES_EQUAL(stats.Blobs, 1u);
        UNIT_ASSERT_VALUES_EQUAL(stats.Parts, 1u);
        UNIT_ASSERT_VALUES_EQUAL(stats.PartsWithDifferingCopies, 0u);
        const TString exportedName = TStringBuilder() << TLogoBlobID(id, 0).ToString() << ".part" << id.PartId();
        UNIT_ASSERT_VALUES_EQUAL(TFileInput(TFsPath(tmp()) / exportedName).ReadAll(), data);
    }

    Y_UNIT_TEST(RepeatedLogSignaturesReportedOnce) {
        // Signatures this tool does not replay must not produce one line per record.
        TYard yard;
        const TVDiskID vdisk(11, 1, 0, 0, 0);
        auto init = yard.Call<TEvYardInitResult>(new TEvYardInit(2, vdisk, yard.Guid));
        const TOwner owner = init->PDiskParams->Owner;
        const TOwnerRound round = init->PDiskParams->OwnerRound;
        const ui32 count = 25;
        for (ui32 i = 0; i < count; ++i) {
            yard.Call<TEvLogResult>(new TEvLog(owner, round, TLogSignature::SignatureLocalSyncData,
                TRcBuf(TString(32, 'S')), TLsnSeg(i + 1, i + 1), nullptr));
        }
        yard.Stop();

        auto session = OpenTool(yard.Map);
        TIssueLog issues;
        ReconstructHull(*session.Device, session.Format, session.State, session.Log, owner,
            TErasureType::ErasureNone, issues);
        const TString name = SignatureName(TLogSignature::SignatureLocalSyncData);
        ui32 mentions = 0;
        for (const auto& i : issues.Items) {
            if (i.Message.find(name) != TString::npos) {
                ++mentions;
                UNIT_ASSERT_C(i.Message.find(ToString(count)) != TString::npos, i.Message);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(mentions, 1u);
    }

    Y_UNIT_TEST(BlobsWithoutDataAreSkipped) {
        // An index record that carries no local data is not what a recovery run is looking for.
        THullSnapshot snap;
        snap.Erasure = TErasureType::ErasureNone;
        const TLogoBlobID withData(500ull, 1, 1, 0, 16, 0);
        const TLogoBlobID withoutData(501ull, 1, 1, 0, 16, 0);

        TBlobIndexEntry empty;
        empty.Id = withoutData;
        snap.Blobs.push_back(empty);

        TBlobIndexEntry full;
        full.Id = withData;
        full.MemRec.SetHugeBlob(TDiskPart(7, 0, 16));
        snap.Blobs.push_back(full);
        std::sort(snap.Blobs.begin(), snap.Blobs.end(),
            [](const TBlobIndexEntry& a, const TBlobIndexEntry& b) { return a.Id < b.Id; });

        TIssueLog issues;
        NKikimr::NPdiskTool::TBlobsResult listed;
        ListBlobs(snap, TListFilter{}, issues, listed);
        UNIT_ASSERT_VALUES_EQUAL(listed.BlobsSize(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(listed.GetBlobs(0).GetLogoBlobId(), withData.ToString());
        UNIT_ASSERT_VALUES_EQUAL(listed.GetSkippedWithoutData(), 1u);

        TListFilter all;
        all.DataOnly = false;
        NKikimr::NPdiskTool::TBlobsResult everything;
        ListBlobs(snap, all, issues, everything);
        UNIT_ASSERT_VALUES_EQUAL(everything.BlobsSize(), 2u);

        // Filters narrow the listing down to one tablet / channel.
        TListFilter byTablet;
        byTablet.TabletId = 500ull;
        NKikimr::NPdiskTool::TBlobsResult filtered;
        ListBlobs(snap, byTablet, issues, filtered);
        UNIT_ASSERT_VALUES_EQUAL(filtered.BlobsSize(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(filtered.GetBlobs(0).GetLogoBlobId(), withData.ToString());

        TListFilter byOtherChannel;
        byOtherChannel.Channel = 5;
        NKikimr::NPdiskTool::TBlobsResult none;
        ListBlobs(snap, byOtherChannel, issues, none);
        UNIT_ASSERT_VALUES_EQUAL(none.BlobsSize(), 0u);
    }

    Y_UNIT_TEST(SeveralCopiesOfPartAreComparedOnExport) {
        TYard yard;
        const TVDiskID vdisk(9, 1, 0, 0, 0);
        auto init = yard.Call<TEvYardInitResult>(new TEvYardInit(2, vdisk, yard.Guid));
        const TOwner owner = init->PDiskParams->Owner;
        const TOwnerRound round = init->PDiskParams->OwnerRound;
        auto reserved = yard.Call<TEvChunkReserveResult>(new TEvChunkReserve(owner, round, 1));
        const TChunkIdx chunk = reserved->ChunkIds[0];
        NPDisk::TCommitRecord commit;
        commit.CommitChunks.push_back(chunk);
        yard.Call<TEvLogResult>(new TEvLog(owner, round, TLogSignature::First, commit,
            TRcBuf(TString()), TLsnSeg(1, 1), nullptr));

        const ui32 blobSize = 16;
        TString payload(24576, 'A');
        // Second half differs, so two locations give two different copies of the same part.
        memset(payload.Detach() + 8192, 'B', payload.size() - 8192);
        yard.Call<TEvChunkWriteResult>(new TEvChunkWrite(owner, round, chunk, 0,
            new TEvChunkWrite::TAlignedParts(TString(payload)), nullptr, true, 1));
        yard.Stop();

        auto session = OpenTool(yard.Map);
        const ui32 payloadSize = session.Format.SectorPayloadSize();

        const TLogoBlobID id(600ull, 1, 1, 0, blobSize, 0);
        const TString partName = TStringBuilder() << id.ToString() << ".part1";
        auto snapWith = [&](ui32 secondOffset) {
            THullSnapshot snap;
            snap.Erasure = TErasureType::ErasureNone;
            for (ui32 offset : {0u, secondOffset}) {
                TBlobIndexEntry e;
                e.Id = id;
                e.MemRec.SetHugeBlob(TDiskPart(chunk, offset, blobSize));
                snap.Blobs.push_back(e);
            }
            return snap;
        };

        // Two records pointing at the very same bytes describe one copy, not two.
        {
            TIssueLog issues;
            NKikimr::NPdiskTool::TBlobsResult listed;
            ListBlobs(snapWith(0), TListFilter{}, issues, listed);
            UNIT_ASSERT_VALUES_EQUAL(listed.BlobsSize(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(listed.GetBlobs(0).PartsSize(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(listed.GetBlobs(0).GetParts(0).GetCopies(), 1u);
        }

        // Two distinct locations holding equal bytes: one file, no complaint.
        {
            TIssueLog issues;
            TTempDir tmp;
            TExportStats stats;
            UNIT_ASSERT(ExportBlobParts(*session.Device, session.Format, session.State,
                snapWith(payloadSize), TLogoBlobID(), {}, TListFilter{}, TString(tmp()), issues, stats));
            UNIT_ASSERT_VALUES_EQUAL(stats.Parts, 1u);
            UNIT_ASSERT_VALUES_EQUAL(stats.PartsWithSeveralCopies, 1u);
            UNIT_ASSERT_VALUES_EQUAL(stats.PartsWithDifferingCopies, 0u);
            UNIT_ASSERT_VALUES_EQUAL(
                TFileInput(TFsPath(tmp()) / partName).ReadAll(), TString(blobSize, 'A'));
        }

        // Copies that disagree are all kept and reported.
        {
            TIssueLog issues;
            TTempDir tmp;
            TExportStats stats;
            UNIT_ASSERT(ExportBlobParts(*session.Device, session.Format, session.State,
                snapWith(payloadSize * 3), TLogoBlobID(), {}, TListFilter{}, TString(tmp()), issues, stats));
            UNIT_ASSERT_VALUES_EQUAL(stats.PartsWithDifferingCopies, 1u);
            UNIT_ASSERT(issues.HasErrors());
            UNIT_ASSERT_VALUES_EQUAL(
                TFileInput(TFsPath(tmp()) / (partName + ".copy1")).ReadAll(), TString(blobSize, 'A'));
            UNIT_ASSERT_VALUES_EQUAL(
                TFileInput(TFsPath(tmp()) / (partName + ".copy2")).ReadAll(), TString(blobSize, 'B'));
        }
    }

    Y_UNIT_TEST(DamagedFormatReplicaStillReadsDisk) {
        const ui64 guid = 0xF00Dull;
        auto map = FormatMap(guid);
        TVector<ui8> sector(NPDisk::FormatSectorSize);
        UNIT_ASSERT(map->Read(sector.data(), NPDisk::FormatSectorSize, 0));
        sector[100] ^= 0xff;
        UNIT_ASSERT(map->Write(sector.data(), NPDisk::FormatSectorSize, 0));

        auto session = OpenTool(map);
        UNIT_ASSERT_VALUES_EQUAL(session.Format.Guid, guid);
        ui32 bad = 0;
        for (const auto& r : session.FormatResult.Replicas) {
            bad += !r.HashOk;
        }
        UNIT_ASSERT(bad >= 1);
        bool warned = false;
        for (const auto& i : session.Issues.Items) {
            if (i.Location == "format") {
                warned = true;
            }
        }
        UNIT_ASSERT(warned);
        UNIT_ASSERT(session.SysLogRaw.Ok);
    }

    Y_UNIT_TEST(DamagedDataSectorReportsGap) {
        TYard yard;
        const TVDiskID vdisk(2, 1, 0, 0, 0);
        auto init = yard.Call<TEvYardInitResult>(new TEvYardInit(2, vdisk, yard.Guid));
        const TOwner owner = init->PDiskParams->Owner;
        const TOwnerRound round = init->PDiskParams->OwnerRound;
        auto reserved = yard.Call<TEvChunkReserveResult>(new TEvChunkReserve(owner, round, 1));
        const TChunkIdx chunk = reserved->ChunkIds[0];
        NPDisk::TCommitRecord commit;
        commit.CommitChunks.push_back(chunk);
        yard.Call<TEvLogResult>(new TEvLog(owner, round, TLogSignature::First, commit,
            TRcBuf(TString()), TLsnSeg(1, 1), nullptr));
        TString payload(8192, 'X');
        yard.Call<TEvChunkWriteResult>(new TEvChunkWrite(owner, round, chunk, 0,
            new TEvChunkWrite::TAlignedParts(TString(payload)), nullptr, true, 1));
        yard.Stop();

        ui64 firstSectorOffset = 0;
        ui32 payloadSize = 0;
        {
            auto session = OpenTool(yard.Map);
            firstSectorOffset = session.Format.Offset(chunk, 0);
            payloadSize = session.Format.SectorPayloadSize();
            TIssueLog issues;
            auto clean = ReadChunkLogical(*session.Device, session.Format, session.State, chunk, false, issues);
            UNIT_ASSERT_VALUES_EQUAL(clean.Data[0], 'X');
        }

        // Corrupt first data sector of the chunk.
        TVector<ui8> sector(4096);
        UNIT_ASSERT(yard.Map->Read(sector.data(), 4096, firstSectorOffset));
        sector[10] ^= 0xff;
        UNIT_ASSERT(yard.Map->Write(sector.data(), 4096, firstSectorOffset));

        auto session = OpenTool(yard.Map);
        TIssueLog issues;
        auto data = ReadChunkLogical(*session.Device, session.Format, session.State, chunk, false, issues);
        UNIT_ASSERT(data.GapCount >= 1);
        // The damaged sector is left as a hole in the logical image; the next one is still intact.
        UNIT_ASSERT_VALUES_EQUAL(data.Data[0], 0);
        UNIT_ASSERT_VALUES_EQUAL(data.Data[payloadSize], 'X');
    }

    Y_UNIT_TEST(UnwrittenSectorsAreQuietReferencedOnesAreReported) {
        TYard yard;
        const TVDiskID vdisk(3, 1, 0, 0, 0);
        auto init = yard.Call<TEvYardInitResult>(new TEvYardInit(2, vdisk, yard.Guid));
        const TOwner owner = init->PDiskParams->Owner;
        const TOwnerRound round = init->PDiskParams->OwnerRound;
        auto reserved = yard.Call<TEvChunkReserveResult>(new TEvChunkReserve(owner, round, 1));
        const TChunkIdx chunk = reserved->ChunkIds[0];
        NPDisk::TCommitRecord commit;
        commit.CommitChunks.push_back(chunk);
        yard.Call<TEvLogResult>(new TEvLog(owner, round, TLogSignature::First, commit,
            TRcBuf(TString()), TLsnSeg(1, 1), nullptr));
        // Only the head of the chunk is written; the tail stays untouched, like a reserved huge slot
        // or the free space in an SST.
        TString payload(8192, 'X');
        yard.Call<TEvChunkWriteResult>(new TEvChunkWrite(owner, round, chunk, 0,
            new TEvChunkWrite::TAlignedParts(TString(payload)), nullptr, true, 1));
        yard.Stop();

        auto hasBadHash = [](const TIssueLog& issues) {
            for (const auto& i : issues.Items) {
                if (i.Message.find("Bad sector hash") != TString::npos) {
                    return true;
                }
            }
            return false;
        };

        ui32 payloadSize = 0;
        ui64 firstSectorOffset = 0;
        {
            auto session = OpenTool(yard.Map);
            payloadSize = session.Format.SectorPayloadSize();
            firstSectorOffset = session.Format.Offset(chunk, 0);

            // Scanning the whole chunk must stay quiet: almost all of it was never written.
            TIssueLog scanIssues;
            auto whole = ReadChunkLogical(*session.Device, session.Format, session.State, chunk, false, scanIssues);
            UNIT_ASSERT(whole.GapCount > 1);
            UNIT_ASSERT_C(!hasBadHash(scanIssues), "whole-chunk scan reported never-written sectors as bad");

            // Reading the written head is a referenced read, and it is intact.
            TIssueLog refIssues;
            TString head = ReadLogicalRange(*session.Device, session.Format, session.State, chunk,
                0, payloadSize, refIssues, "blob");
            UNIT_ASSERT_VALUES_EQUAL(head.size(), payloadSize);
            UNIT_ASSERT_VALUES_EQUAL(head[0], 'X');
            UNIT_ASSERT_C(!hasBadHash(refIssues), "intact referenced range reported as bad");

            // Checking a range that reaches into the unwritten tail does count bad sectors: something
            // points there, so the data is genuinely missing.
            TIssueLog tailIssues;
            auto tail = CheckLogicalRange(*session.Device, session.Format, session.State, chunk,
                payloadSize * 4, payloadSize, tailIssues, "verify-ref");
            UNIT_ASSERT_VALUES_EQUAL(tail.Checked, 1u);
            UNIT_ASSERT_VALUES_EQUAL(tail.Bad, 1u);
            UNIT_ASSERT(hasBadHash(tailIssues));
        }

        // Corrupt the first (written, referenced) sector.
        TVector<ui8> sector(4096);
        UNIT_ASSERT(yard.Map->Read(sector.data(), 4096, firstSectorOffset));
        sector[10] ^= 0xff;
        UNIT_ASSERT(yard.Map->Write(sector.data(), 4096, firstSectorOffset));

        auto session = OpenTool(yard.Map);
        TIssueLog refIssues;
        ReadLogicalRange(*session.Device, session.Format, session.State, chunk, 0, payloadSize, refIssues, "blob");
        UNIT_ASSERT_C(hasBadHash(refIssues), "damaged referenced sector was not reported");

        TIssueLog scanIssues;
        ReadChunkLogical(*session.Device, session.Format, session.State, chunk, false, scanIssues);
        UNIT_ASSERT_C(!hasBadHash(scanIssues), "whole-chunk scan must not warn per sector");
    }

    Y_UNIT_TEST(EncryptionOffFormat) {
        auto map = MakeIntrusive<NPDisk::TSectorMap>(DiskSize);
        TFormatOptions options;
        options.SectorMap = map;
        options.EnableSmallDiskOptimization = true;
        options.EnableFormatAndMetadataEncryption = false;
        options.EnableSectorEncryption = false;
        FormatPDisk("", DiskSize, 4096, ChunkSize, 42,
            NPDisk::TKey(1), NPDisk::TKey(2), NPDisk::TKey(3), MainKeyValue, "plain", options);
        auto session = OpenTool(map);
        UNIT_ASSERT_VALUES_EQUAL(session.Format.Guid, 42u);
        UNIT_ASSERT(session.SysLogRaw.Ok);
    }

    Y_UNIT_TEST(PlainDataChunksAndFileDevice) {
        auto map = MakeIntrusive<NPDisk::TSectorMap>(DiskSize);
        TFormatOptions options;
        options.SectorMap = map;
        options.EnableSmallDiskOptimization = true;
        options.PlainDataChunks = true;
        FormatPDisk("", DiskSize, 4096, ChunkSize, 77,
            NPDisk::TKey(1), NPDisk::TKey(2), NPDisk::TKey(3), MainKeyValue, "plain-data", options);
        auto session = OpenTool(map);
        UNIT_ASSERT_VALUES_EQUAL(session.Format.Guid, 77u);
        UNIT_ASSERT(session.Format.IsPlainDataChunks());

        TTempDir tmp;
        const ui32 fileChunk = 8 << 20;
        const ui64 fileDisk = ui64(fileChunk) * 80;
        TString path = TString(tmp()) + "/pdisk.bin";
        TFormatOptions fileOpts;
        fileOpts.EnableSmallDiskOptimization = true;
        FormatPDisk(path, fileDisk, 4096, fileChunk, 88,
            NPDisk::TKey(1), NPDisk::TKey(2), NPDisk::TKey(3), MainKeyValue, "file", fileOpts);
        TPDiskSession fileSession;
        UNIT_ASSERT(fileSession.OpenFile(path, DefaultOpts()));
        UNIT_ASSERT_VALUES_EQUAL(fileSession.Format.Guid, 88u);
        UNIT_ASSERT(fileSession.SysLogRaw.Ok);
    }

    Y_UNIT_TEST(MainKeyArgAndKeyConfigProto) {
        UNIT_ASSERT_VALUES_EQUAL(ParseMainKeyArg("YdbDefaultPDiskSequence"), NPDisk::YdbDefaultPDiskSequence);
        UNIT_ASSERT_VALUES_EQUAL(ParseMainKeyArg("default"), NPDisk::YdbDefaultPDiskSequence);
        UNIT_ASSERT_VALUES_EQUAL(ParseMainKeyArg("0x7e5700007e570000"), NPDisk::YdbDefaultPDiskSequence);
        UNIT_ASSERT_VALUES_EQUAL(ParseMainKeyArg("0X7E5700007E570000"), NPDisk::YdbDefaultPDiskSequence);
        UNIT_ASSERT_VALUES_EQUAL(ParseMainKeyArg("1"), 1u);

        TTempDir tmp;
        const TString container = TString(tmp()) + "/pdisk.key";
        {
            TFileOutput out(container);
            out << "pdisktool-container-bytes";
        }
        const TString protoPath = TString(tmp()) + "/pdisk_key.txt";
        {
            TFileOutput out(protoPath);
            out << "Keys {\n"
                << "  ContainerPath: \"" << container << "\"\n"
                << "  Id: \"pdisk\"\n"
                << "  Version: 1\n"
                << "}\n";
        }

        TIssueLog protoIssues;
        const auto fromProto = MakeMainKey({}, protoPath, "", true, protoIssues);
        UNIT_ASSERT_C(!protoIssues.HasErrors(), protoIssues.Items.empty() ? "" : protoIssues.Items.back().Message);
        UNIT_ASSERT_VALUES_EQUAL(fromProto.Keys.size(), 1u);

        TIssueLog rawIssues;
        const auto fromRaw = MakeMainKey({}, container, "", true, rawIssues);
        UNIT_ASSERT_C(!rawIssues.HasErrors(), rawIssues.Items.empty() ? "" : rawIssues.Items.back().Message);
        UNIT_ASSERT_VALUES_EQUAL(fromRaw.Keys.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(fromProto.Keys[0], fromRaw.Keys[0]);

        auto map = MakeIntrusive<NPDisk::TSectorMap>(DiskSize);
        TFormatOptions options;
        options.SectorMap = map;
        options.EnableSmallDiskOptimization = true;
        FormatPDisk("", DiskSize, 4096, ChunkSize, 0x42,
            NPDisk::TKey(1), NPDisk::TKey(2), NPDisk::TKey(3), fromProto.Keys[0], "key-config", options);

        TPDiskSession ok;
        TSessionOptions opts;
        opts.MainKey = fromProto;
        opts.TryLock = false;
        UNIT_ASSERT(ok.OpenSectorMap(map, opts));
        UNIT_ASSERT(ok.FormatResult.Ok);
        UNIT_ASSERT_VALUES_EQUAL(ok.Format.Guid, 0x42u);

        TPDiskSession badDefault;
        UNIT_ASSERT(!badDefault.OpenSectorMap(map, DefaultOpts()));
        UNIT_ASSERT(!badDefault.FormatResult.Ok);

        const TString brokenProto = TString(tmp()) + "/broken.txt";
        {
            TFileOutput out(brokenProto);
            out << "Keys {\n  ContainerPath: \n";
        }
        TIssueLog brokenIssues;
        MakeMainKey({}, brokenProto, "", true, brokenIssues);
        UNIT_ASSERT(brokenIssues.HasErrors());
    }

    Y_UNIT_TEST(ImplausibleLogPageSizesAreReportedAndScanTerminates) {
        TYard yard;
        const TVDiskID vdisk(31, 1, 0, 0, 0);
        auto init = yard.Call<TEvYardInitResult>(new TEvYardInit(2, vdisk, yard.Guid));
        const TOwner owner = init->PDiskParams->Owner;
        const TOwnerRound round = init->PDiskParams->OwnerRound;
        for (ui32 i = 0; i < 4; ++i) {
            yard.Call<TEvLogResult>(new TEvLog(owner, round, TLogSignature::First,
                TRcBuf(TString(64, 'L')), TLsnSeg(i + 1, i + 1), nullptr));
        }
        yard.Stop();

        TDiskFormat format;
        ui64 sectorOffset = 0;
        ui32 pageOffset = 0;
        TVector<ui8> pristine;
        {
            auto session = OpenTool(yard.Map);
            format = session.Format;
            const TChunkIdx logChunk = session.State.Record.LogHeadChunkIdx;
            TIssueLog issues;
            bool found = false;
            for (ui32 s = 0; s < 8 && !found; ++s) {
                const ui64 offset = format.Offset(logChunk, s);
                auto restored = RestoreOneSector(*session.Device, format, offset, format.MagicLogChunk,
                    format.LogKey, true, issues, "ut", {}, ESectorRef::Unreferenced);
                if (!restored.Ok) {
                    continue;
                }
                const ui32 page = FindFirstLogPage(restored.Payload.data(), restored.Payload.size());
                if (page != Max<ui32>()) {
                    sectorOffset = offset;
                    pageOffset = page;
                    found = true;
                }
            }
            UNIT_ASSERT_C(found, "no first log page to corrupt");
            pristine.resize(format.SectorSize);
            UNIT_ASSERT(yard.Map->Read(pristine.data(), format.SectorSize, sectorOffset));
        }

        auto scanWithPatchedPage = [&](auto mutate) {
            UNIT_ASSERT(yard.Map->Write(pristine.data(), format.SectorSize, sectorOffset));
            RewriteSector(*yard.Map, format, sectorOffset, sectorOffset, format.MagicLogChunk, format.LogKey,
                [&](ui8* payload) { mutate(reinterpret_cast<TFirstLogPageHeader*>(payload + pageOffset)); });
            TPDiskSession session;
            UNIT_ASSERT(session.OpenSectorMap(yard.Map, DefaultOpts()));
            return session;
        };

        {
            // A page claiming more bytes than the sector holds must not be copied out of the sector.
            auto session = scanWithPatchedPage([](TFirstLogPageHeader* h) { h->Size = 0x7fff0000; });
            UNIT_ASSERT_C(HasMessage(session.Issues, "exceeds the sector", "log"),
                "oversized page size not reported");
        }
        {
            // A record longer than the whole log chunk must not become an allocation.
            auto session = scanWithPatchedPage([](TFirstLogPageHeader* h) { h->DataSize = Max<ui64>(); });
            UNIT_ASSERT_C(HasMessage(session.Issues, "DataSize is implausible", "log"),
                "implausible DataSize not reported");
        }
        {
            // A zero-size page must not stall the walk over the sector; returning at all is the check.
            auto session = scanWithPatchedPage([](TFirstLogPageHeader* h) {
                h->Size = 0;
                h->DataSize = 0;
            });
            UNIT_ASSERT(session.SysLogRaw.Ok);
        }
    }

    Y_UNIT_TEST(ImplausibleSysLogPageSizeIsCapped) {
        auto map = FormatMap(0x5150ull);
        TDiskFormat format;
        ui32 setIdx = 0;
        {
            auto session = OpenTool(map);
            format = session.Format;
            ui64 bestNonce = 0;
            bool found = false;
            for (const auto& info : session.SysLogRaw.SectorSets) {
                if (info.HasStart && info.Nonce >= bestNonce) {
                    bestNonce = info.Nonce;
                    setIdx = info.SetIdx;
                    found = true;
                }
            }
            UNIT_ASSERT_C(found, "no SysLog sector set with a first page");
        }

        // All three replicas of a set are identical and hash against the set's own offset.
        const ui64 base = ui64(format.FirstSysLogSectorIdx() + setIdx * NPDisk::ReplicationFactor)
            * format.SectorSize;
        for (ui32 replica = 0; replica < NPDisk::ReplicationFactor; ++replica) {
            RewriteSector(*map, format, base, base + ui64(replica) * format.SectorSize,
                format.MagicSysLogChunk, format.SysLogKey, [](ui8* payload) {
                    reinterpret_cast<TFirstLogPageHeader*>(payload)->DataSize = Max<ui64>();
                });
        }

        TPDiskSession session;
        UNIT_ASSERT(session.OpenSectorMap(map, DefaultOpts()));
        UNIT_ASSERT_C(HasMessage(session.Issues, "DataSize is implausible", "syslog"),
            "implausible SysLog DataSize not reported");
    }

    Y_UNIT_TEST(HullWithoutOwnerTableIsRefused) {
        // Every starting point comes from the SysLog, so with no owner table there is nothing to walk.
        auto session = OpenTool(FormatMap(0x7717ull));
        TParsedSysLog noState;
        TIssueLog issues;
        auto snap = ReconstructHull(*session.Device, session.Format, noState, TLogScanResult{}, 1,
            TErasureType::ErasureNone, issues);
        UNIT_ASSERT(snap.Blobs.empty());
        UNIT_ASSERT(issues.HasErrors());
        UNIT_ASSERT(HasMessage(issues, "No owner table"));
    }

    Y_UNIT_TEST(HostileSstPlaceholderIsAbandoned) {
        TYard yard;
        const TVDiskID vdisk(41, 1, 0, 0, 0);
        auto init = yard.Call<TEvYardInitResult>(new TEvYardInit(2, vdisk, yard.Guid));
        const TOwner owner = init->PDiskParams->Owner;
        const TOwnerRound round = init->PDiskParams->OwnerRound;
        auto reserved = yard.Call<TEvChunkReserveResult>(new TEvChunkReserve(owner, round, 1));
        const TChunkIdx chunk = reserved->ChunkIds[0];
        NPDisk::TCommitRecord commit;
        commit.CommitChunks.push_back(chunk);
        yard.Call<TEvLogResult>(new TEvLog(owner, round, TLogSignature::First, commit,
            TRcBuf(TString()), TLsnSeg(1, 1), nullptr));

        const ui32 fragmentSize = 8192;
        TString image(2 * fragmentSize, 'q');
        {
            // Sizes far beyond what the fragment carries.
            TIdxDiskPlaceHolder ph(1);
            ph.Info.Items = Max<ui32>();
            ph.Info.IdxTotalSize = Max<ui32>();
            ph.Info.OutboundItems = Max<ui32>();
            memcpy(image.Detach() + fragmentSize - sizeof(ph), &ph, sizeof(ph));
        }
        {
            // A fragment chain that links back to itself.
            TIdxDiskPlaceHolder ph(2);
            ph.Info.Items = 1;
            ph.Info.IdxTotalSize = sizeof(TIndexRecord<TKeyLogoBlob, TMemRecLogoBlob>);
            ph.PrevPart = TDiskPart(chunk, fragmentSize, fragmentSize);
            memcpy(image.Detach() + 2 * fragmentSize - sizeof(ph), &ph, sizeof(ph));
        }
        yard.Call<TEvChunkWriteResult>(new TEvChunkWrite(owner, round, chunk, 0,
            new TEvChunkWrite::TAlignedParts(TString(image)), nullptr, true, 1));

        NKikimrVDiskData::THullDbEntryPoint pb;
        auto* level0 = pb.MutableLevelIndex()->MutableLevel0();
        TDiskPart(chunk, 0, fragmentSize).SerializeToProto(*level0->AddSsts());
        TDiskPart(chunk, fragmentSize, fragmentSize).SerializeToProto(*level0->AddSsts());
        TString body;
        UNIT_ASSERT(pb.SerializeToString(&body));
        const ui32 hullEntryMagic = 0x93F7ADD5;
        TString entry = TString::Uninitialized(sizeof(hullEntryMagic) + body.size());
        memcpy(entry.Detach(), &hullEntryMagic, sizeof(hullEntryMagic));
        memcpy(entry.Detach() + sizeof(hullEntryMagic), body.data(), body.size());
        NPDisk::TCommitRecord startingPoint;
        startingPoint.IsStartingPoint = true;
        yard.Call<TEvLogResult>(new TEvLog(owner, round, TLogSignature::SignatureHullLogoBlobsDB,
            startingPoint, TRcBuf(entry), TLsnSeg(2, 2), nullptr));
        yard.Stop();

        auto session = OpenTool(yard.Map);
        TIssueLog issues;
        auto snap = ReconstructHull(*session.Device, session.Format, session.State, session.Log, owner,
            TErasureType::ErasureNone, issues);
        UNIT_ASSERT(snap.Blobs.empty());
        UNIT_ASSERT_C(HasMessage(issues, "disagrees with the fragments", "hull"),
            "hostile placeholder not reported");
        UNIT_ASSERT_C(HasMessage(issues, "loops at", "hull"),
            "self-referencing fragment chain not reported");
    }

    Y_UNIT_TEST(NewerFormatVersionIsReportedNotAborted) {
        auto map = FormatMap(0x1234ull);
        const ui32 total = NPDisk::FormatSectorSize * NPDisk::ReplicationFactor;
        TVector<ui8> raw(total);
        UNIT_ASSERT(map->Read(raw.data(), total, 0));
        for (ui32 i = 0; i < NPDisk::ReplicationFactor; ++i) {
            ui8* sector = raw.data() + i * NPDisk::FormatSectorSize;
            const auto* footer = reinterpret_cast<const TDataSectorFooter*>(
                sector + NPDisk::FormatSectorSize - sizeof(TDataSectorFooter));
            const ui64 nonce = footer->Nonce;
            TPDiskStreamCypher cypher(true);
            cypher.SetKey(MainKeyValue);
            alignas(16) NPDisk::TDiskFormatSector plain;
            cypher.StartMessage(nonce);
            cypher.Encrypt(plain.Raw, sector, NPDisk::FormatSectorSize);
            UNIT_ASSERT(plain.Format.IsHashOk(NPDisk::FormatSectorSize));
            plain.Format.Version = PDISK_FORMAT_VERSION + 1;
            plain.Format.SetHash();
            cypher.StartMessage(nonce);
            cypher.Encrypt(sector, plain.Raw, NPDisk::FormatSectorSize);
        }
        UNIT_ASSERT(map->Write(raw.data(), total, 0));

        TPDiskSession session;
        UNIT_ASSERT(!session.OpenSectorMap(map, DefaultOpts()));
        UNIT_ASSERT(!session.FormatResult.Ok);
        UNIT_ASSERT_C(HasMessage(session.Issues, "newer than this build supports", "format"),
            "a format from a newer build must be reported, not asserted on");
    }

    Y_UNIT_TEST(BadBlobIdFilterFailsTheCommand) {
        TTempDir tmp;
        const TString path = TString(tmp()) + "/pdisk.bin";
        const ui32 chunkSize = 8 << 20;
        TFormatOptions options;
        options.EnableSmallDiskOptimization = true;
        FormatPDisk(path, ui64(chunkSize) * 80, 4096, chunkSize, 99,
            NPDisk::TKey(1), NPDisk::TKey(2), NPDisk::TKey(3), MainKeyValue, "filter", options);

        auto run = [&](const TString& from) {
            TVector<TString> args = {"blobs", "--device", path, "--main-key", "YdbDefaultPDiskSequence",
                "--owner", "3", "--from", from};
            TVector<char*> argv;
            for (auto& a : args) {
                argv.push_back(const_cast<char*>(a.c_str()));
            }
            return RunCommand(args[0], argv.size(), argv.data());
        };
        UNIT_ASSERT_VALUES_UNEQUAL(run("not-a-blob-id"), 0);
    }

    Y_UNIT_TEST(FillFormatProtoOnFailedReadDoesNotCrash) {
        TFormatReadResult result;
        result.Ok = false;
        TFormatReplicaInfo replica;
        replica.Index = 0;
        replica.Nonce = 1;
        replica.Decrypted = true;
        replica.Error = "hash mismatch nonce# 1";
        result.Replicas.push_back(replica);

        NKikimr::NPdiskTool::TFormatResult proto;
        FillFormatProto(result, proto, false);
        UNIT_ASSERT(!proto.HasFormat());
        UNIT_ASSERT_VALUES_EQUAL(proto.ReplicasSize(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(proto.GetReplicas(0).GetNonce(), 1u);

        TStringStream out;
        PrintFormatText(proto, out);
        UNIT_ASSERT(out.Str().Contains("nonce=1"));
    }
}
