#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

struct TCheckIntegrityEnvBase {
    TEnvironmentSetup Env;
    TIntrusivePtr<TBlobStorageGroupInfo> Info;
    TLogoBlobID Id;
    std::vector<TVDiskID> VDisks;

    std::unique_ptr<IEventHandle> Result;

    TCheckIntegrityEnvBase(TEnvironmentSetup::TSettings&& settings)
        : Env(std::move(settings))
    {
        Env.CreateBoxAndPool(1, 1);
        Env.Sim(TDuration::Minutes(1));

        auto groups = Env.GetGroups();
        UNIT_ASSERT(groups.size() == 1);
        Info = Env.GetGroupInfo(groups.front());

        TString error;
        const bool success = TLogoBlobID::Parse(Id, "[72075186270680851:57:3905:6:786432:4194304:0]", error);
        UNIT_ASSERT(success);
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

struct TCheckIntegrityEnvBlock42 : public TCheckIntegrityEnvBase {
    std::vector<TString> Parts;

    TCheckIntegrityEnvBlock42()
        : TCheckIntegrityEnvBase(TEnvironmentSetup::TSettings{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        })
    {
        TString data = TString(Id.BlobSize(), 'X');

        TDataPartSet partSet;
        Info->Type.SplitData((TErasureType::ECrcMode)Id.CrcMode(), data, partSet);
        for (ui32 i = 0; i < partSet.Parts.size(); ++i) {
            Parts.push_back(partSet.Parts[i].OwnedString.ConvertToString());
        }

        for (ui32 i = 0; i < 8; ++i) {
            auto vDiskIdShort = Info->GetTopology().GetVDiskInSubgroup(i, Id.Hash());
            VDisks.push_back(Info->CreateVDiskID(vDiskIdShort));
        }
    }
};

struct TCheckIntegrityEnvMirror3dc : public TCheckIntegrityEnvBase {
    TString Data;

    TCheckIntegrityEnvMirror3dc()
        : TCheckIntegrityEnvBase(TEnvironmentSetup::TSettings{
            .NodeCount = 9,
            .Erasure = TBlobStorageGroupType::ErasureMirror3dc,
        })
    {
        Data = TString(Id.BlobSize(), 'X');

        for (ui32 i = 0; i < 9; ++i) {
            auto vDiskIdShort = Info->GetTopology().GetVDiskInSubgroup(i, Id.Hash());
            VDisks.push_back(Info->CreateVDiskID(vDiskIdShort));
        }
    }
};

Y_UNIT_TEST_SUITE(CheckIntegrityBlock42) {

    Y_UNIT_TEST(PlacementOk) {
        TCheckIntegrityEnvBlock42 check;

        for (ui32 i = 0; i < 6; ++i) {
            check.Env.PutBlob(check.VDisks[i], TLogoBlobID(check.Id, i + 1), check.Parts[i]);
        }

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_OK);
    }

    Y_UNIT_TEST(PlacementOkHandoff) {
        TCheckIntegrityEnvBlock42 check;

        for (ui32 i = 2; i < 6; ++i) {
            check.Env.PutBlob(check.VDisks[i], TLogoBlobID(check.Id, i + 1), check.Parts[i]);
        }
        check.Env.PutBlob(check.VDisks[6], TLogoBlobID(check.Id, 1), check.Parts[0]);
        check.Env.PutBlob(check.VDisks[7], TLogoBlobID(check.Id, 2), check.Parts[1]);

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_OK);
    }

    Y_UNIT_TEST(PlacementMissingParts) {
        TCheckIntegrityEnvBlock42 check;

        for (ui32 i = 2; i < 6; ++i) {
            check.Env.PutBlob(check.VDisks[i], TLogoBlobID(check.Id, i + 1), check.Parts[i]);
        }

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_RECOVERABLE);
    }

    Y_UNIT_TEST(PlacementBlobIsLost) {
        TCheckIntegrityEnvBlock42 check;

        for (ui32 i = 0; i < 3; ++i) {
            check.Env.PutBlob(check.VDisks[i], TLogoBlobID(check.Id, i + 1), check.Parts[i]);
        }

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_ERROR);
    }

    Y_UNIT_TEST(PlacementWrongDisks) {
        TCheckIntegrityEnvBlock42 check;

        for (ui32 i = 2; i < 6; ++i) {
            check.Env.PutBlob(check.VDisks[i], TLogoBlobID(check.Id, i + 1), check.Parts[i]);
        }
        check.Env.PutBlob(check.VDisks[6], TLogoBlobID(check.Id, 1), check.Parts[0]);
        check.Env.PutBlob(check.VDisks[6], TLogoBlobID(check.Id, 2), check.Parts[1]);

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_RECOVERABLE);
    }

    Y_UNIT_TEST(PlacementAllOnHandoff) {
        TCheckIntegrityEnvBlock42 check;

        for (ui32 i = 0; i < 6; ++i) {
            check.Env.PutBlob(check.VDisks[6], TLogoBlobID(check.Id, i + 1), check.Parts[i]);
        }

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_RECOVERABLE);
    }

    Y_UNIT_TEST(PlacementDisintegrated) {
        TCheckIntegrityEnvBlock42 check;

        for (ui32 i = 0; i < 6; ++i) {
            check.Env.PutBlob(check.VDisks[i], TLogoBlobID(check.Id, i + 1), check.Parts[i]);
        }

        THashSet<TVDiskID> errorDisks;
        errorDisks.insert(check.VDisks[5]);
        errorDisks.insert(check.VDisks[6]);
        errorDisks.insert(check.VDisks[7]);

        check.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            return check.InjectError(NKikimrProto::ERROR, errorDisks, ev);
        };

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::ERROR);
        Cerr << result->ErrorReason << Endl;
    }

    Y_UNIT_TEST(PlacementOkWithErrors) {
        TCheckIntegrityEnvBlock42 check;

        for (ui32 i = 0; i < 6; ++i) {
            check.Env.PutBlob(check.VDisks[i], TLogoBlobID(check.Id, i + 1), check.Parts[i]);
        }

        THashSet<TVDiskID> errorDisks;
        errorDisks.insert(check.VDisks[6]);
        errorDisks.insert(check.VDisks[7]);

        check.Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            return check.InjectError(NKikimrProto::ERROR, errorDisks, ev);
        };

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_OK);
    }

    Y_UNIT_TEST(PlacementWithErrorsOnBlobDisks) {
        TCheckIntegrityEnvBlock42 check;

        for (ui32 i = 0; i < 6; ++i) {
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
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_RECOVERABLE);
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
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_RECOVERABLE);
    }

    Y_UNIT_TEST(PlacementBlobIsLost) {
        TCheckIntegrityEnvMirror3dc check;

        auto result = check.Request();
        UNIT_ASSERT(result->Status == NKikimrProto::OK);
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_ERROR);
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
        UNIT_ASSERT(result->PlacementStatus == TEvBlobStorage::TEvCheckIntegrityResult::PS_RECOVERABLE);
    }
}
