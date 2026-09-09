#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

#include <util/random/random.h>

struct TCheckIntegrityEnvBase {
    TEnvironmentSetup Env;
    TIntrusivePtr<TBlobStorageGroupInfo> Info;
    TLogoBlobID Id;
    std::vector<TVDiskID> VDisks;

    TString Data;
    TString ErrorData;

    std::unique_ptr<IEventHandle> Result;

    TCheckIntegrityEnvBase(TEnvironmentSetup::TSettings&& settings, ui32 crcMode = 0)
        : Env(std::move(settings))
    {
        Env.CreateBoxAndPool(1, 1);
        Env.Sim(TDuration::Minutes(1));

        auto groups = Env.GetGroups();
        UNIT_ASSERT(groups.size() == 1);
        Info = Env.GetGroupInfo(groups.front());

        TString error;
        const bool success = TLogoBlobID::Parse(Id, "[72075186270680851:57:3905:6:786432:1024:0]", error);
        UNIT_ASSERT(success);
        Id = TLogoBlobID(Id.TabletID(), Id.Generation(), Id.Step(), Id.Channel(),
            Id.BlobSize(), Id.Cookie(), 0, crcMode);

        auto size = Id.BlobSize();
        Data.resize(size);
        for (ui32 i = 0; i < size; ++i) {
            Data[i] = RandomNumber<ui8>();
        }
        ErrorData.resize(size);
        for (ui32 i = 0; i < size; ++i) {
            ErrorData[i] = RandomNumber<ui8>();
        }

        for (ui32 i = 0; i < Info->Type.BlobSubgroupSize(); ++i) {
            auto vDiskIdShort = Info->GetTopology().GetVDiskInSubgroup(i, Id.Hash());
            VDisks.push_back(Info->CreateVDiskID(vDiskIdShort));
        }
    }

    TEvBlobStorage::TEvCheckIntegrityResult* Request() {
        const auto edge = Env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);

        Env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, Info->GroupID, new TEvBlobStorage::TEvCheckIntegrity
                (Id, TInstant::Max(), NKikimrBlobStorage::EGetHandleClass::FastRead));
        });

        Result.reset(Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCheckIntegrityResult>(edge).Release());
        return Result->Get<TEvBlobStorage::TEvCheckIntegrityResult>();
    }

    bool InjectError(
        NKikimrProto::EReplyStatus status,
        const THashSet<TVDiskID>& errorDisks,
        std::unique_ptr<IEventHandle>& ev)
    {
        if (ev->GetTypeRewrite() == TEvBlobStorage::EvVGet) {
            auto* msg = ev->Get<TEvBlobStorage::TEvVGet>();
            auto vDiskId = VDiskIDFromVDiskID(msg->Record.GetVDiskID());

            if (!errorDisks.contains(vDiskId)) {
                return true;
            }

            auto result = std::make_unique<TEvBlobStorage::TEvVGetResult>();
            auto& record = result->Record;
            record.SetStatus(status);

            VDiskIDFromVDiskID(vDiskId, record.MutableVDiskID());

            Env.Runtime->Send(
                new IEventHandle(ev->Sender, ev->Recipient, result.release(), 0, ev->Cookie),
                ev->Sender.NodeId());
            return false;
        }
        return true;
    }
};

struct TCheckIntegrityEnvParityBlock : public TCheckIntegrityEnvBase {
    std::vector<TString> Parts;
    std::vector<TString> ErrorParts;

    TCheckIntegrityEnvParityBlock(TErasureType::EErasureSpecies species, ui32 crcModeRaw = 0)
        : TCheckIntegrityEnvBase(TEnvironmentSetup::TSettings{
            .NodeCount = TBlobStorageGroupType(species).BlobSubgroupSize(),
            .Erasure = species,
        }, crcModeRaw)
    {
        auto crcMode = (TErasureType::ECrcMode)Id.CrcMode();

        TDataPartSet partSet;
        Info->Type.SplitData(crcMode, Data, partSet);
        for (ui32 i = 0; i < partSet.Parts.size(); ++i) {
            Parts.push_back(partSet.Parts[i].OwnedString.ConvertToString());
        }

        TDataPartSet errorPartSet;
        Info->Type.SplitData(crcMode, ErrorData, errorPartSet);
        for (ui32 i = 0; i < errorPartSet.Parts.size(); ++i) {
            ErrorParts.push_back(errorPartSet.Parts[i].OwnedString.ConvertToString());
        }
    }
};

struct TCheckIntegrityEnvMirror3dc : public TCheckIntegrityEnvBase {
    TCheckIntegrityEnvMirror3dc()
        : TCheckIntegrityEnvBase(TEnvironmentSetup::TSettings{
            .NodeCount = 9,
            .Erasure = TBlobStorageGroupType::ErasureMirror3dc,
        })
    {}
};

struct TCheckIntegrityEnvMirror3of4 : public TCheckIntegrityEnvBase {

    TCheckIntegrityEnvMirror3of4()
        : TCheckIntegrityEnvBase(TEnvironmentSetup::TSettings{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::ErasureMirror3of4,
        })
    {}
};

namespace NCheckIntegrityParityBlock {
    void PlacementOk(TErasureType::EErasureSpecies species, ui32 crcMode = 0) {
        TCheckIntegrityEnvParityBlock check(species, crcMode);
        const ui32 total = check.Info->Type.TotalPartCount();
        const ui32 required = check.Info->Type.DataParts();
        Y_UNUSED(total);
        Y_UNUSED(required);

        for (ui32 i = 0; i < total; ++i) {
            check.Env.PutBlob(check.VDisks[i], TLogoBlobID(check.Id, i + 1), check.Parts[i]);
        }

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_OK);
        }
    void PlacementOkHandoff(TErasureType::EErasureSpecies species, ui32 crcMode = 0) {
        TCheckIntegrityEnvParityBlock check(species, crcMode);
        const ui32 total = check.Info->Type.TotalPartCount();
        const ui32 required = check.Info->Type.DataParts();
        Y_UNUSED(total);
        Y_UNUSED(required);

        for (ui32 i = 2; i < total; ++i) {
            check.Env.PutBlob(check.VDisks[i], TLogoBlobID(check.Id, i + 1), check.Parts[i]);
        }
        check.Env.PutBlob(check.VDisks[total], TLogoBlobID(check.Id, 1), check.Parts[0]);
        check.Env.PutBlob(check.VDisks[(total + 1)], TLogoBlobID(check.Id, 2), check.Parts[1]);

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_OK);
        }
    void PlacementMissingParts(TErasureType::EErasureSpecies species, ui32 crcMode = 0) {
        TCheckIntegrityEnvParityBlock check(species, crcMode);
        const ui32 total = check.Info->Type.TotalPartCount();
        const ui32 required = check.Info->Type.DataParts();
        Y_UNUSED(total);
        Y_UNUSED(required);

        for (ui32 i = 2; i < total; ++i) {
            check.Env.PutBlob(check.VDisks[i], TLogoBlobID(check.Id, i + 1), check.Parts[i]);
        }

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_BLOB_IS_RECOVERABLE);
        }
    void PlacementBlobIsLost(TErasureType::EErasureSpecies species, ui32 crcMode = 0) {
        TCheckIntegrityEnvParityBlock check(species, crcMode);
        const ui32 total = check.Info->Type.TotalPartCount();
        const ui32 required = check.Info->Type.DataParts();
        Y_UNUSED(total);
        Y_UNUSED(required);

        for (ui32 i = 0; i < (required - 1); ++i) {
            check.Env.PutBlob(check.VDisks[i], TLogoBlobID(check.Id, i + 1), check.Parts[i]);
        }

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_BLOB_IS_LOST);
        }
    void PlacementWrongDisks(TErasureType::EErasureSpecies species, ui32 crcMode = 0) {
        TCheckIntegrityEnvParityBlock check(species, crcMode);
        const ui32 total = check.Info->Type.TotalPartCount();
        const ui32 required = check.Info->Type.DataParts();
        Y_UNUSED(total);
        Y_UNUSED(required);

        for (ui32 i = 2; i < total; ++i) {
            check.Env.PutBlob(check.VDisks[i], TLogoBlobID(check.Id, i + 1), check.Parts[i]);
        }
        check.Env.PutBlob(check.VDisks[total], TLogoBlobID(check.Id, 1), check.Parts[0]);
        check.Env.PutBlob(check.VDisks[total], TLogoBlobID(check.Id, 2), check.Parts[1]);

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_BLOB_IS_RECOVERABLE);
        }
    void PlacementAllOnHandoff(TErasureType::EErasureSpecies species, ui32 crcMode = 0) {
        TCheckIntegrityEnvParityBlock check(species, crcMode);
        const ui32 total = check.Info->Type.TotalPartCount();
        const ui32 required = check.Info->Type.DataParts();
        Y_UNUSED(total);
        Y_UNUSED(required);

        for (ui32 i = 0; i < total; ++i) {
            check.Env.PutBlob(check.VDisks[total], TLogoBlobID(check.Id, i + 1), check.Parts[i]);
        }

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_BLOB_IS_RECOVERABLE);
        }
    void PlacementDisintegrated(TErasureType::EErasureSpecies species, ui32 crcMode = 0) {
        TCheckIntegrityEnvParityBlock check(species, crcMode);
        const ui32 total = check.Info->Type.TotalPartCount();
        const ui32 required = check.Info->Type.DataParts();
        Y_UNUSED(total);
        Y_UNUSED(required);

        for (ui32 i = 0; i < total; ++i) {
            check.Env.PutBlob(check.VDisks[i], TLogoBlobID(check.Id, i + 1), check.Parts[i]);
        }

        THashSet<TVDiskID> errorDisks;
        errorDisks.insert(check.VDisks[(total - 1)]);
        errorDisks.insert(check.VDisks[total]);
        errorDisks.insert(check.VDisks[(total + 1)]);

        check.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            return check.InjectError(NKikimrProto::ERROR, errorDisks, ev);
        };

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::ERROR);
        Cerr << result->ErrorReason << Endl;
        }
    void PlacementOkWithErrors(TErasureType::EErasureSpecies species, ui32 crcMode = 0) {
        TCheckIntegrityEnvParityBlock check(species, crcMode);
        const ui32 total = check.Info->Type.TotalPartCount();
        const ui32 required = check.Info->Type.DataParts();
        Y_UNUSED(total);
        Y_UNUSED(required);

        for (ui32 i = 0; i < total; ++i) {
            check.Env.PutBlob(check.VDisks[i], TLogoBlobID(check.Id, i + 1), check.Parts[i]);
        }

        THashSet<TVDiskID> errorDisks;
        errorDisks.insert(check.VDisks[total]);
        errorDisks.insert(check.VDisks[(total + 1)]);

        check.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            return check.InjectError(NKikimrProto::ERROR, errorDisks, ev);
        };

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_OK);
        }
    void PlacementWithErrorsOnBlobDisks(TErasureType::EErasureSpecies species, ui32 crcMode = 0) {
        TCheckIntegrityEnvParityBlock check(species, crcMode);
        const ui32 total = check.Info->Type.TotalPartCount();
        const ui32 required = check.Info->Type.DataParts();
        Y_UNUSED(total);
        Y_UNUSED(required);

        for (ui32 i = 0; i < total; ++i) {
            check.Env.PutBlob(check.VDisks[i], TLogoBlobID(check.Id, i + 1), check.Parts[i]);
        }

        THashSet<TVDiskID> errorDisks;
        errorDisks.insert(check.VDisks[0]);
        errorDisks.insert(check.VDisks[1]);

        check.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            return check.InjectError(NKikimrProto::ERROR, errorDisks, ev);
        };

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_BLOB_IS_RECOVERABLE);
        }
    void PlacementStatusUnknown(TErasureType::EErasureSpecies species, ui32 crcMode = 0) {
        TCheckIntegrityEnvParityBlock check(species, crcMode);
        const ui32 total = check.Info->Type.TotalPartCount();
        const ui32 required = check.Info->Type.DataParts();
        Y_UNUSED(total);
        Y_UNUSED(required);

        for (ui32 i = 0; i < (total - 1); ++i) {
            check.Env.PutBlob(check.VDisks[i], TLogoBlobID(check.Id, i + 1), check.Parts[i]);
        }

        THashSet<TVDiskID> errorDisks;
        errorDisks.insert(check.VDisks[0]);
        errorDisks.insert(check.VDisks[1]);

        check.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            return check.InjectError(NKikimrProto::ERROR, errorDisks, ev);
        };

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_UNKNOWN);
        }
    void DataOk(TErasureType::EErasureSpecies species, ui32 crcMode = 0) {
        TCheckIntegrityEnvParityBlock check(species, crcMode);
        const ui32 total = check.Info->Type.TotalPartCount();
        const ui32 required = check.Info->Type.DataParts();
        Y_UNUSED(total);
        Y_UNUSED(required);

        for (ui32 i = 0; i < total; ++i) {
            check.Env.PutBlob(check.VDisks[i], TLogoBlobID(check.Id, i + 1), check.Parts[i]);
        }

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_OK);
        UNIT_ASSERT(result->DataStatus == TEvBlobStorage::TEvCheckIntegrityResult::DS_OK);

        Cerr << result->DataInfo << Endl;
        }
    void DataOkAdditionalEqualParts(TErasureType::EErasureSpecies species, ui32 crcMode = 0) {
        TCheckIntegrityEnvParityBlock check(species, crcMode);
        const ui32 total = check.Info->Type.TotalPartCount();
        const ui32 required = check.Info->Type.DataParts();
        Y_UNUSED(total);
        Y_UNUSED(required);

        for (ui32 i = 0; i < total; ++i) {
            check.Env.PutBlob(check.VDisks[i], TLogoBlobID(check.Id, i + 1), check.Parts[i]);
        }
        check.Env.PutBlob(check.VDisks[total], TLogoBlobID(check.Id, 1), check.Parts[0]);
        check.Env.PutBlob(check.VDisks[(total + 1)], TLogoBlobID(check.Id, 2), check.Parts[1]);

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_OK);
        UNIT_ASSERT(result->DataStatus == TEvBlobStorage::TEvCheckIntegrityResult::DS_OK);

        Cerr << result->DataInfo << Endl;
        }
    void DataErrorAdditionalUnequalParts(TErasureType::EErasureSpecies species, ui32 crcMode = 0) {
        TCheckIntegrityEnvParityBlock check(species, crcMode);
        const ui32 total = check.Info->Type.TotalPartCount();
        const ui32 required = check.Info->Type.DataParts();
        Y_UNUSED(total);
        Y_UNUSED(required);

        for (ui32 i = 0; i < total; ++i) {
            check.Env.PutBlob(check.VDisks[i], TLogoBlobID(check.Id, i + 1), check.Parts[i]);
        }
        check.Env.PutBlob(check.VDisks[total], TLogoBlobID(check.Id, 1), check.ErrorParts[0]);
        check.Env.PutBlob(check.VDisks[(total + 1)], TLogoBlobID(check.Id, 1), check.ErrorParts[1]);

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_OK);
        UNIT_ASSERT(result->DataStatus == TEvBlobStorage::TEvCheckIntegrityResult::DS_ERROR);

        Cerr << result->DataInfo << Endl;
        }
    void DataErrorSixPartsOneBroken(TErasureType::EErasureSpecies species, ui32 crcMode = 0) {
        TCheckIntegrityEnvParityBlock check(species, crcMode);
        const ui32 total = check.Info->Type.TotalPartCount();
        const ui32 required = check.Info->Type.DataParts();
        Y_UNUSED(total);
        Y_UNUSED(required);

        for (ui32 i = 0; i < (total - 1); ++i) {
            check.Env.PutBlob(check.VDisks[i], TLogoBlobID(check.Id, i + 1), check.Parts[i]);
        }
        check.Env.PutBlob(check.VDisks[(total - 1)], TLogoBlobID(check.Id, total), check.ErrorParts[(total - 1)]);

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_OK);
        UNIT_ASSERT(result->DataStatus == TEvBlobStorage::TEvCheckIntegrityResult::DS_ERROR);

        Cerr << result->DataInfo << Endl;
        }
    void DataErrorSixPartsTwoBroken(TErasureType::EErasureSpecies species, ui32 crcMode = 0) {
        TCheckIntegrityEnvParityBlock check(species, crcMode);
        const ui32 total = check.Info->Type.TotalPartCount();
        const ui32 required = check.Info->Type.DataParts();
        Y_UNUSED(total);
        Y_UNUSED(required);

        for (ui32 i = 0; i < required; ++i) {
            check.Env.PutBlob(check.VDisks[i], TLogoBlobID(check.Id, i + 1), check.Parts[i]);
        }
        check.Env.PutBlob(check.VDisks[required], TLogoBlobID(check.Id, (total - 1)), check.ErrorParts[required]);
        check.Env.PutBlob(check.VDisks[(total - 1)], TLogoBlobID(check.Id, total), check.ErrorParts[(total - 1)]);

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_OK);
        UNIT_ASSERT(result->DataStatus == TEvBlobStorage::TEvCheckIntegrityResult::DS_ERROR);

        Cerr << result->DataInfo << Endl;
        }
    void DataOkErasureFiveParts(TErasureType::EErasureSpecies species, ui32 crcMode = 0) {
        TCheckIntegrityEnvParityBlock check(species, crcMode);
        const ui32 total = check.Info->Type.TotalPartCount();
        const ui32 required = check.Info->Type.DataParts();
        Y_UNUSED(total);
        Y_UNUSED(required);

        for (ui32 i = 0; i < (total - 1); ++i) {
            check.Env.PutBlob(check.VDisks[i], TLogoBlobID(check.Id, i + 1), check.Parts[i]);
        }

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_BLOB_IS_RECOVERABLE);
        UNIT_ASSERT(result->DataStatus == TEvBlobStorage::TEvCheckIntegrityResult::DS_OK);

        Cerr << result->DataInfo << Endl;
        }
    void DataErrorFivePartsOneBroken(TErasureType::EErasureSpecies species, ui32 crcMode = 0) {
        TCheckIntegrityEnvParityBlock check(species, crcMode);
        const ui32 total = check.Info->Type.TotalPartCount();
        const ui32 required = check.Info->Type.DataParts();
        Y_UNUSED(total);
        Y_UNUSED(required);

        for (ui32 i = 0; i < required; ++i) {
            check.Env.PutBlob(check.VDisks[i], TLogoBlobID(check.Id, i + 1), check.Parts[i]);
        }
        check.Env.PutBlob(check.VDisks[required], TLogoBlobID(check.Id, (total - 1)), check.ErrorParts[required]);

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_BLOB_IS_RECOVERABLE);
        UNIT_ASSERT(result->DataStatus == TEvBlobStorage::TEvCheckIntegrityResult::DS_ERROR);

        Cerr << result->DataInfo << Endl;
        }
    void DataErrorHeavySixPartsWithManyBroken(TErasureType::EErasureSpecies species, ui32 crcMode = 0) {
        TCheckIntegrityEnvParityBlock check(species, crcMode);
        const ui32 total = check.Info->Type.TotalPartCount();
        const ui32 required = check.Info->Type.DataParts();
        Y_UNUSED(total);
        Y_UNUSED(required);

        for (ui32 i = 0; i < total; ++i) {
            check.Env.PutBlob(check.VDisks[i], TLogoBlobID(check.Id, i + 1), check.Parts[i]);
        }
        for (ui32 i = 0; i < total; ++i) {
            check.Env.PutBlob(check.VDisks[total], TLogoBlobID(check.Id, i + 1), check.ErrorParts[i]);
        }

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_OK);
        UNIT_ASSERT(result->DataStatus == TEvBlobStorage::TEvCheckIntegrityResult::DS_ERROR);

        Cerr << result->DataInfo << Endl;
        }
    void DataStatusUnknown(TErasureType::EErasureSpecies species, ui32 crcMode = 0) {
        TCheckIntegrityEnvParityBlock check(species, crcMode);
        const ui32 total = check.Info->Type.TotalPartCount();
        const ui32 required = check.Info->Type.DataParts();
        Y_UNUSED(total);
        Y_UNUSED(required);

        for (ui32 i = 0; i < (total - 1); ++i) {
            check.Env.PutBlob(check.VDisks[i], TLogoBlobID(check.Id, i + 1), check.Parts[i]);
        }

        THashSet<TVDiskID> errorDisks;
        errorDisks.insert(check.VDisks[0]);
        errorDisks.insert(check.VDisks[1]);

        check.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            return check.InjectError(NKikimrProto::ERROR, errorDisks, ev);
        };

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_UNKNOWN);
        UNIT_ASSERT(result->DataStatus == TEvBlobStorage::TEvCheckIntegrityResult::DS_UNKNOWN);

        Cerr << result->DataInfo << Endl;
        }
}

Y_UNIT_TEST_SUITE(CheckIntegrityBlock42) {
    Y_UNIT_TEST(PlacementOk) { NCheckIntegrityParityBlock::PlacementOk(TErasureType::Erasure4Plus2Block); }
    Y_UNIT_TEST(PlacementOkHandoff) { NCheckIntegrityParityBlock::PlacementOkHandoff(TErasureType::Erasure4Plus2Block); }
    Y_UNIT_TEST(PlacementMissingParts) { NCheckIntegrityParityBlock::PlacementMissingParts(TErasureType::Erasure4Plus2Block); }
    Y_UNIT_TEST(PlacementBlobIsLost) { NCheckIntegrityParityBlock::PlacementBlobIsLost(TErasureType::Erasure4Plus2Block); }
    Y_UNIT_TEST(PlacementWrongDisks) { NCheckIntegrityParityBlock::PlacementWrongDisks(TErasureType::Erasure4Plus2Block); }
    Y_UNIT_TEST(PlacementAllOnHandoff) { NCheckIntegrityParityBlock::PlacementAllOnHandoff(TErasureType::Erasure4Plus2Block); }
    Y_UNIT_TEST(PlacementDisintegrated) { NCheckIntegrityParityBlock::PlacementDisintegrated(TErasureType::Erasure4Plus2Block); }
    Y_UNIT_TEST(PlacementOkWithErrors) { NCheckIntegrityParityBlock::PlacementOkWithErrors(TErasureType::Erasure4Plus2Block); }
    Y_UNIT_TEST(PlacementWithErrorsOnBlobDisks) { NCheckIntegrityParityBlock::PlacementWithErrorsOnBlobDisks(TErasureType::Erasure4Plus2Block); }
    Y_UNIT_TEST(PlacementStatusUnknown) { NCheckIntegrityParityBlock::PlacementStatusUnknown(TErasureType::Erasure4Plus2Block); }
    Y_UNIT_TEST(DataOk) { NCheckIntegrityParityBlock::DataOk(TErasureType::Erasure4Plus2Block); }
    Y_UNIT_TEST(DataOkAdditionalEqualParts) { NCheckIntegrityParityBlock::DataOkAdditionalEqualParts(TErasureType::Erasure4Plus2Block); }
    Y_UNIT_TEST(DataErrorAdditionalUnequalParts) { NCheckIntegrityParityBlock::DataErrorAdditionalUnequalParts(TErasureType::Erasure4Plus2Block); }
    Y_UNIT_TEST(DataErrorSixPartsOneBroken) { NCheckIntegrityParityBlock::DataErrorSixPartsOneBroken(TErasureType::Erasure4Plus2Block); }
    Y_UNIT_TEST(DataErrorSixPartsTwoBroken) { NCheckIntegrityParityBlock::DataErrorSixPartsTwoBroken(TErasureType::Erasure4Plus2Block); }
    Y_UNIT_TEST(DataOkErasureFiveParts) { NCheckIntegrityParityBlock::DataOkErasureFiveParts(TErasureType::Erasure4Plus2Block); }
    Y_UNIT_TEST(DataErrorFivePartsOneBroken) { NCheckIntegrityParityBlock::DataErrorFivePartsOneBroken(TErasureType::Erasure4Plus2Block); }
    Y_UNIT_TEST(DataErrorHeavySixPartsWithManyBroken) { NCheckIntegrityParityBlock::DataErrorHeavySixPartsWithManyBroken(TErasureType::Erasure4Plus2Block); }
    Y_UNIT_TEST(DataStatusUnknown) { NCheckIntegrityParityBlock::DataStatusUnknown(TErasureType::Erasure4Plus2Block); }
}

Y_UNIT_TEST_SUITE(CheckIntegrityBlock82) {
    Y_UNIT_TEST(MalformedPartReply) {
        for (auto crc : {TErasureType::CrcModeNone, TErasureType::CrcModeWholePart}) {
            TCheckIntegrityEnvParityBlock check(TErasureType::Erasure8Plus2Block, crc);
            for (ui32 part = 0; part < check.Parts.size(); ++part) {
                check.Env.PutBlob(check.VDisks[part], TLogoBlobID(check.Id, part + 1), check.Parts[part]);
            }
            for (ui32 partId : {1u, 8u, 9u, 10u}) {
                for (ui32 badSize : {1u, ui32(check.Parts[partId - 1].size() - 1), ui32(check.Parts[partId - 1].size() + 1)}) {
                    bool injected = false;
                    check.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& event) {
                        if (event->GetTypeRewrite() == TEvBlobStorage::EvVGetResult) {
                            auto* reply = event->Get<TEvBlobStorage::TEvVGetResult>();
                            for (auto& item : *reply->Record.MutableResult()) {
                                if (item.GetStatus() == NKikimrProto::OK &&
                                        LogoBlobIDFromLogoBlobID(item.GetBlobID()) == TLogoBlobID(check.Id, partId)) {
                                    item.ClearPayloadId();
                                    item.SetBufferData(TString(badSize, 'x'));
                                    item.SetSize(badSize);
                                    injected = true;
                                }
                            }
                        }
                        return true;
                    };
                    const auto* result = check.Request();
                    UNIT_ASSERT(injected);
                    UNIT_ASSERT_VALUES_EQUAL(result->Status, NKikimrProto::OK);
                    UNIT_ASSERT(result->DataStatus == TEvBlobStorage::TEvCheckIntegrityResult::DS_ERROR);
                    check.Env.Runtime->FilterFunction = {};
                }
            }
        }
    }
    Y_UNIT_TEST(PlacementOk) { NCheckIntegrityParityBlock::PlacementOk(TErasureType::Erasure8Plus2Block); }
    Y_UNIT_TEST(PlacementOkHandoff) { NCheckIntegrityParityBlock::PlacementOkHandoff(TErasureType::Erasure8Plus2Block); }
    Y_UNIT_TEST(PlacementMissingParts) { NCheckIntegrityParityBlock::PlacementMissingParts(TErasureType::Erasure8Plus2Block); }
    Y_UNIT_TEST(PlacementBlobIsLost) { NCheckIntegrityParityBlock::PlacementBlobIsLost(TErasureType::Erasure8Plus2Block); }
    Y_UNIT_TEST(PlacementWrongDisks) { NCheckIntegrityParityBlock::PlacementWrongDisks(TErasureType::Erasure8Plus2Block); }
    Y_UNIT_TEST(PlacementAllOnHandoff) { NCheckIntegrityParityBlock::PlacementAllOnHandoff(TErasureType::Erasure8Plus2Block); }
    Y_UNIT_TEST(PlacementDisintegrated) { NCheckIntegrityParityBlock::PlacementDisintegrated(TErasureType::Erasure8Plus2Block); }
    Y_UNIT_TEST(PlacementOkWithErrors) { NCheckIntegrityParityBlock::PlacementOkWithErrors(TErasureType::Erasure8Plus2Block); }
    Y_UNIT_TEST(PlacementWithErrorsOnBlobDisks) { NCheckIntegrityParityBlock::PlacementWithErrorsOnBlobDisks(TErasureType::Erasure8Plus2Block); }
    Y_UNIT_TEST(PlacementStatusUnknown) { NCheckIntegrityParityBlock::PlacementStatusUnknown(TErasureType::Erasure8Plus2Block); }
    Y_UNIT_TEST(DataOk) { NCheckIntegrityParityBlock::DataOk(TErasureType::Erasure8Plus2Block); }
    Y_UNIT_TEST(DataOkAdditionalEqualParts) { NCheckIntegrityParityBlock::DataOkAdditionalEqualParts(TErasureType::Erasure8Plus2Block); }
    Y_UNIT_TEST(DataErrorAdditionalUnequalParts) { NCheckIntegrityParityBlock::DataErrorAdditionalUnequalParts(TErasureType::Erasure8Plus2Block); }
    Y_UNIT_TEST(DataErrorSixPartsOneBroken) { NCheckIntegrityParityBlock::DataErrorSixPartsOneBroken(TErasureType::Erasure8Plus2Block); }
    Y_UNIT_TEST(DataErrorSixPartsTwoBroken) { NCheckIntegrityParityBlock::DataErrorSixPartsTwoBroken(TErasureType::Erasure8Plus2Block); }
    Y_UNIT_TEST(DataOkErasureFiveParts) { NCheckIntegrityParityBlock::DataOkErasureFiveParts(TErasureType::Erasure8Plus2Block); }
    Y_UNIT_TEST(DataErrorFivePartsOneBroken) { NCheckIntegrityParityBlock::DataErrorFivePartsOneBroken(TErasureType::Erasure8Plus2Block); }
    Y_UNIT_TEST(DataErrorHeavySixPartsWithManyBroken) { NCheckIntegrityParityBlock::DataErrorHeavySixPartsWithManyBroken(TErasureType::Erasure8Plus2Block); }
    Y_UNIT_TEST(DataStatusUnknown) { NCheckIntegrityParityBlock::DataStatusUnknown(TErasureType::Erasure8Plus2Block); }
    Y_UNIT_TEST(WholePartCRC) {
        NCheckIntegrityParityBlock::DataOk(TErasureType::Erasure8Plus2Block, TErasureType::CrcModeWholePart);
        NCheckIntegrityParityBlock::DataOkAdditionalEqualParts(TErasureType::Erasure8Plus2Block, TErasureType::CrcModeWholePart);
        NCheckIntegrityParityBlock::DataErrorAdditionalUnequalParts(TErasureType::Erasure8Plus2Block, TErasureType::CrcModeWholePart);
        NCheckIntegrityParityBlock::DataErrorSixPartsOneBroken(TErasureType::Erasure8Plus2Block, TErasureType::CrcModeWholePart);
        NCheckIntegrityParityBlock::DataErrorSixPartsTwoBroken(TErasureType::Erasure8Plus2Block, TErasureType::CrcModeWholePart);
        NCheckIntegrityParityBlock::DataOkErasureFiveParts(TErasureType::Erasure8Plus2Block, TErasureType::CrcModeWholePart);
        NCheckIntegrityParityBlock::DataErrorFivePartsOneBroken(TErasureType::Erasure8Plus2Block, TErasureType::CrcModeWholePart);
    }
}

Y_UNIT_TEST_SUITE(CheckIntegrityMirror3dc) {

    Y_UNIT_TEST(PlacementOk) {
        TCheckIntegrityEnvMirror3dc check;

        for (ui32 i = 0; i < 3; ++i) {
            check.Env.PutBlob(check.VDisks[i], TLogoBlobID(check.Id, i + 1), check.Data);
        }

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_OK);
    }

    Y_UNIT_TEST(PlacementOkHandoff) {
        TCheckIntegrityEnvMirror3dc check;

        for (ui32 i = 0; i < 3; ++i) {
            check.Env.PutBlob(check.VDisks[i + 3], TLogoBlobID(check.Id, i + 1), check.Data);
        }

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_OK);
    }

    Y_UNIT_TEST(PlacementMissingParts) {
        TCheckIntegrityEnvMirror3dc check;

        check.Env.PutBlob(check.VDisks[0], TLogoBlobID(check.Id, 1), check.Data);

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_BLOB_IS_RECOVERABLE);
    }

    Y_UNIT_TEST(PlacementBlobIsLost) {
        TCheckIntegrityEnvMirror3dc check;

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_BLOB_IS_LOST);
    }

    Y_UNIT_TEST(PlacementDisintegrated) {
        TCheckIntegrityEnvMirror3dc check;

        for (ui32 i = 0; i < 3; ++i) {
            check.Env.PutBlob(check.VDisks[i], TLogoBlobID(check.Id, i + 1), check.Data);
        }

        THashSet<TVDiskID> errorDisks;
        for (ui32 i = 4; i < 9; ++i) {
            errorDisks.insert(check.VDisks[i]);
        }

        check.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            return check.InjectError(NKikimrProto::ERROR, errorDisks, ev);
        };

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::ERROR);
        Cerr << result->ErrorReason << Endl;
    }

    Y_UNIT_TEST(PlacementOkWithErrors) {
        TCheckIntegrityEnvMirror3dc check;

        for (ui32 i = 0; i < 3; ++i) {
            check.Env.PutBlob(check.VDisks[i], TLogoBlobID(check.Id, i + 1), check.Data);
        }

        THashSet<TVDiskID> errorDisks;
        errorDisks.insert(check.VDisks[5]);
        errorDisks.insert(check.VDisks[7]);
        errorDisks.insert(check.VDisks[8]);

        check.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            return check.InjectError(NKikimrProto::ERROR, errorDisks, ev);
        };

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_OK);
    }

    Y_UNIT_TEST(PlacementOkWithErrorsOnBlobDisks) {
        TCheckIntegrityEnvMirror3dc check;

        for (ui32 i = 0; i < 3; ++i) {
            check.Env.PutBlob(check.VDisks[i], TLogoBlobID(check.Id, i + 1), check.Data);
        }

        THashSet<TVDiskID> errorDisks;
        errorDisks.insert(check.VDisks[0]);
        errorDisks.insert(check.VDisks[1]);

        check.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            return check.InjectError(NKikimrProto::ERROR, errorDisks, ev);
        };

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_BLOB_IS_RECOVERABLE);
    }

    Y_UNIT_TEST(DataOk) {
        TCheckIntegrityEnvMirror3dc check;

        for (ui32 i = 0; i < 3; ++i) {
            check.Env.PutBlob(check.VDisks[i], TLogoBlobID(check.Id, i + 1), check.Data);
        }

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_OK);
        UNIT_ASSERT(result->DataStatus == TEvBlobStorage::TEvCheckIntegrityResult::DS_OK);

        Cerr << result->DataInfo << Endl;
    }

    Y_UNIT_TEST(DataErrorOneCopy) {
        TCheckIntegrityEnvMirror3dc check;

        for (ui32 i = 0; i < 2; ++i) {
            check.Env.PutBlob(check.VDisks[i], TLogoBlobID(check.Id, i + 1), check.Data);
        }
        check.Env.PutBlob(check.VDisks[2], TLogoBlobID(check.Id, 3), check.ErrorData);

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_OK);
        UNIT_ASSERT(result->DataStatus == TEvBlobStorage::TEvCheckIntegrityResult::DS_ERROR);

        Cerr << result->DataInfo << Endl;
    }

    Y_UNIT_TEST(DataErrorManyCopies) {
        TCheckIntegrityEnvMirror3dc check;

        for (ui32 i = 0; i < 3; ++i) {
            check.Env.PutBlob(check.VDisks[i], TLogoBlobID(check.Id, i + 1), check.Data);
        }
        for (ui32 i = 0; i < 3; ++i) {
            check.Env.PutBlob(check.VDisks[i + 3], TLogoBlobID(check.Id, i + 1), check.ErrorData);
        }

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_OK);
        UNIT_ASSERT(result->DataStatus == TEvBlobStorage::TEvCheckIntegrityResult::DS_ERROR);

        Cerr << result->DataInfo << Endl;
    }
}

Y_UNIT_TEST_SUITE(CheckIntegrityMirror3of4) {

    Y_UNIT_TEST(PlacementOk) {
        TCheckIntegrityEnvMirror3of4 check;

        check.Env.PutBlob(check.VDisks[0], TLogoBlobID(check.Id, 1), check.Data);
        check.Env.PutBlob(check.VDisks[1], TLogoBlobID(check.Id, 2), check.Data);
        check.Env.PutBlob(check.VDisks[2], TLogoBlobID(check.Id, 1), check.Data);
        check.Env.PutBlob(check.VDisks[4], TLogoBlobID(check.Id, 3), {});
        check.Env.PutBlob(check.VDisks[5], TLogoBlobID(check.Id, 3), {});

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_OK);
    }

    Y_UNIT_TEST(PlacementMissingParts) {
        TCheckIntegrityEnvMirror3of4 check;

        check.Env.PutBlob(check.VDisks[0], TLogoBlobID(check.Id, 1), check.Data);

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_BLOB_IS_RECOVERABLE);
    }

    Y_UNIT_TEST(PlacementBlobIsLost) {
        TCheckIntegrityEnvMirror3of4 check;

        check.Env.PutBlob(check.VDisks[4], TLogoBlobID(check.Id, 3), {});
        check.Env.PutBlob(check.VDisks[5], TLogoBlobID(check.Id, 3), {});

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_BLOB_IS_LOST);
    }

    Y_UNIT_TEST(PlacementDisintegrated) {
        TCheckIntegrityEnvMirror3of4 check;

        THashSet<TVDiskID> errorDisks;
        for (ui32 i = 5; i < 8; ++i) {
            errorDisks.insert(check.VDisks[i]);
        }

        check.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            return check.InjectError(NKikimrProto::ERROR, errorDisks, ev);
        };

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::ERROR);
        Cerr << result->ErrorReason << Endl;
    }
}
