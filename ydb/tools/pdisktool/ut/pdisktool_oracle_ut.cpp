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
#include <ydb/tools/pdisktool/lib/format.h>
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
        ListBlobs(snap, TListFilter{}, blobs);
        UNIT_ASSERT_VALUES_EQUAL(blobs.BlobsSize(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(blobs.GetBlobs(0).GetTabletId(), 1001u);
        UNIT_ASSERT_VALUES_EQUAL(blobs.GetBlobs(0).GetBlobSize(), 16u);

        NKikimr::NPdiskTool::TBlocksResult blocks;
        ListBlocks(snap, TListFilter{}, blocks);
        UNIT_ASSERT_VALUES_EQUAL(blocks.BlocksSize(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(blocks.GetBlocks(0).GetTabletId(), 42u);
        UNIT_ASSERT_VALUES_EQUAL(blocks.GetBlocks(0).GetBlockedGeneration(), 3u);

        TTempDir tmp;
        ui32 exported = 0;
        UNIT_ASSERT(ExportBlobParts(*session.Device, session.Format, session.State, snap,
            TLogoBlobID(1001ull, 1, 1, 0, 16, 0), {}, TString(tmp()), issues, exported));
        UNIT_ASSERT_VALUES_EQUAL(exported, 1u);
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
            auto tail = CheckLogicalRange(*session.Device, session.Format, chunk,
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
