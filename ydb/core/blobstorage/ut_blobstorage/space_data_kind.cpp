#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_util_space_color.h>
#include <ydb/core/base/blobstorage_data_kind.h>
#include <ydb/core/base/blobstorage_write_source.h>

#include <util/random/random.h>

namespace {

using TColor = NKikimrBlobStorage::TPDiskSpaceColor;
using TDataKind = NKikimrBlobStorage::TDataKind;

// Drives a whole block-4-2 group into an arbitrary space color and then writes blobs of a chosen
// data kind straight into a VDisk. Going through the queue actor rather than through DSProxy keeps
// the erasure quorum out of the picture, so every reply is the VDisk admission decision itself.
struct TSpaceColorEnv {
    TEnvironmentSetup Env;
    TIntrusivePtr<TBlobStorageGroupInfo> Info;
    TString Data;
    TString Part;
    ui32 NextStep = 1;

    TSpaceColorEnv()
        : Env(TEnvironmentSetup::TSettings{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        })
    {
        Env.CreateBoxAndPool(1, 1);
        Env.Sim(TDuration::Minutes(1));

        auto groups = Env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        Info = Env.GetGroupInfo(groups.front());

        Data.resize(1024);
        for (size_t i = 0; i < Data.size(); ++i) {
            Data[i] = RandomNumber<ui8>();
        }

        TDataPartSet partSet;
        Info->Type.SplitData((TErasureType::ECrcMode)MakeBlobId(0).CrcMode(), Data, partSet);
        Part = partSet.Parts[0].OwnedString.ConvertToString();
    }

    TLogoBlobID MakeBlobId(ui32 step) const {
        return TLogoBlobID(1, 1, step, 0, Data.size(), 0);
    }

    void SetColor(TColor::E color) {
        for (ui32 i = 0; i < Info->GetTotalVDisksNum(); ++i) {
            ui32 nodeId, pdiskId;
            std::tie(nodeId, pdiskId, std::ignore) = DecomposeVDiskServiceId(Info->GetActorId(i));
            Env.SetPDiskStatusFlags(nodeId, pdiskId, color);
        }
        Env.Sim(TDuration::Seconds(15));
    }

    // Writes a brand new blob every time, so that a repeated write never collapses into ALREADY.
    NKikimrProto::EReplyStatus Put(TDataKind::E dataKind, bool ignoreBlock = false) {
        const TLogoBlobID fullId = MakeBlobId(NextStep++);
        const TLogoBlobID partId(fullId, 1);
        const TVDiskID vdiskId = Info->CreateVDiskID(Info->GetTopology().GetVDiskInSubgroup(0, fullId.Hash()));

        NKikimrProto::EReplyStatus status = NKikimrProto::UNKNOWN;
        Env.WithQueueId(vdiskId, NKikimrBlobStorage::EVDiskQueueId::PutTabletLog, [&](TActorId queueId) {
            const TActorId edge = Env.Runtime->AllocateEdgeActor(queueId.NodeId(), __FILE__, __LINE__);
            Env.Runtime->Send(new IEventHandle(queueId, edge, new TEvBlobStorage::TEvVPut(partId, TRope(Part),
                vdiskId, ignoreBlock, nullptr, TInstant::Max(), NKikimrBlobStorage::EPutHandleClass::TabletLog,
                false, TWriteSource::Unknown, dataKind)), queueId.NodeId());
            status = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVPutResult>(edge)->Get()->Record.GetStatus();
        });
        return status;
    }
};

// Leaves a blob in the group with only its data parts written, so that reading it back with
// MustRestoreFirst makes DSProxy reconstruct and write out the parities.
struct TPartialBlobEnv {
    TEnvironmentSetup Env;
    TIntrusivePtr<TBlobStorageGroupInfo> Info;
    TString Data;
    ui32 NextStep = 1;

    TPartialBlobEnv()
        : Env(TEnvironmentSetup::TSettings{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        })
    {
        Env.CreateBoxAndPool(1, 1);
        Env.Sim(TDuration::Minutes(1));

        auto groups = Env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        Info = Env.GetGroupInfo(groups.front());

        Data.resize(1024);
        for (size_t i = 0; i < Data.size(); ++i) {
            Data[i] = RandomNumber<ui8>();
        }
    }

    TLogoBlobID WriteDataPartsOnly() {
        const TLogoBlobID fullId(1, 1, NextStep++, 0, Data.size(), 0);

        TDataPartSet partSet;
        Info->Type.SplitData((TErasureType::ECrcMode)fullId.CrcMode(), Data, partSet);

        for (ui32 part = 1; part <= Info->Type.DataParts(); ++part) {
            const TVDiskID vdiskId = Info->CreateVDiskID(Info->GetTopology().GetVDiskInSubgroup(part - 1,
                fullId.Hash()));
            Env.PutBlob(vdiskId, TLogoBlobID(fullId, part),
                partSet.Parts[part - 1].OwnedString.ConvertToString());
        }

        return fullId;
    }

    // Reads the blob back through DSProxy and returns the data kind of every write the read
    // provoked.
    std::vector<TDataKind::E> RestoreWriteKinds(TLogoBlobID id, TDataKind::E dataKind) {
        std::vector<TDataKind::E> kinds;
        Env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvVPut) {
                const auto& record = ev->Get<TEvBlobStorage::TEvVPut>()->Record;
                if (WriteSourceFromProto(record.GetWriteSourceOp()) == TWriteSource::DSProxyGetAccelerate) {
                    kinds.push_back(record.GetDataKind());
                }
            }
            return true;
        };

        const TActorId sender = Env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        Env.Runtime->WrapInActorContext(sender, [&] {
            auto ev = std::make_unique<TEvBlobStorage::TEvGet>(id, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::FastRead, true /*mustRestoreFirst*/);
            ev->DataKind = dataKind;
            SendToBSProxy(sender, Info->GroupID, ev.release());
        });
        auto res = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender, false);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->ResponseSz, 1);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses[0].Buffer.ConvertToString(), Data);

        Env.Runtime->FilterFunction = nullptr;
        return kinds;
    }
};

} // namespace

Y_UNIT_TEST_SUITE(SpaceDataKind) {

    // Protobuf enums have no Out<> overload, so these use UNIT_ASSERT_EQUAL, which compares without
    // stringifying the values.
    Y_UNIT_TEST(TabletTypeClassification) {
        UNIT_ASSERT_EQUAL(DataKindByTabletType(TTabletTypes::SchemeShard), TDataKind::SYSTEM);
        UNIT_ASSERT_EQUAL(DataKindByTabletType(TTabletTypes::Hive), TDataKind::SYSTEM);
        UNIT_ASSERT_EQUAL(DataKindByTabletType(TTabletTypes::Coordinator), TDataKind::SYSTEM);
        UNIT_ASSERT_EQUAL(DataKindByTabletType(TTabletTypes::BSController), TDataKind::SYSTEM);

        // BlobDepot implements a whole group, and its index is what records the barriers and the
        // trash, so it has to keep writing for anything in that group to be deletable.
        UNIT_ASSERT_EQUAL(DataKindByTabletType(TTabletTypes::BlobDepot), TDataKind::SYSTEM);

        UNIT_ASSERT_EQUAL(DataKindByTabletType(TTabletTypes::DataShard), TDataKind::USER);
        UNIT_ASSERT_EQUAL(DataKindByTabletType(TTabletTypes::ColumnShard), TDataKind::USER);
        UNIT_ASSERT_EQUAL(DataKindByTabletType(TTabletTypes::PersQueue), TDataKind::USER);
        UNIT_ASSERT_EQUAL(DataKindByTabletType(TTabletTypes::KeyValue), TDataKind::USER);

        // Anything unrecognized must fall back to the stricter, pre-existing behaviour.
        UNIT_ASSERT_EQUAL(DataKindByTabletType(TTabletTypes::Unknown), TDataKind::USER);
        UNIT_ASSERT_EQUAL(DataKindByTabletType(TTabletTypes::UserTypeStart), TDataKind::USER);
    }

    Y_UNIT_TEST(StopColorFollowsDataKind) {
        UNIT_ASSERT_EQUAL(StopWritingStatusFlag(TDataKind::USER),
            NKikimrBlobStorage::StatusDiskSpaceYellowStop);
        UNIT_ASSERT_EQUAL(StopWritingStatusFlag(TDataKind::SYSTEM),
            NKikimrBlobStorage::StatusDiskSpaceOrange);
    }

    // A color reaches the VDisk encoded as status flags, so a color that does not survive that
    // round trip silently degrades into whichever tier it decodes as, taking its admission rules
    // with it. PRE_ORANGE used to decode as LIGHT_ORANGE for exactly this reason.
    Y_UNIT_TEST(SpaceColorSurvivesStatusFlagRoundTrip) {
        for (auto color : {TColor::GREEN, TColor::CYAN, TColor::LIGHT_YELLOW, TColor::YELLOW,
                TColor::LIGHT_ORANGE, TColor::PRE_ORANGE, TColor::ORANGE, TColor::RED, TColor::BLACK}) {
            UNIT_ASSERT_EQUAL_C(StatusFlagToSpaceColor(SpaceColorToStatusFlag(color)), color,
                TPDiskSpaceColor_Name(color));
        }
    }

    Y_UNIT_TEST(BothKindsAcceptedUntilLightOrange) {
        TSpaceColorEnv env;
        for (auto color : {TColor::GREEN, TColor::YELLOW, TColor::LIGHT_ORANGE}) {
            env.SetColor(color);
            UNIT_ASSERT_VALUES_EQUAL_C(env.Put(TDataKind::USER), NKikimrProto::OK, TPDiskSpaceColor_Name(color));
            UNIT_ASSERT_VALUES_EQUAL_C(env.Put(TDataKind::SYSTEM), NKikimrProto::OK, TPDiskSpaceColor_Name(color));
        }
    }

    Y_UNIT_TEST(SystemWritesOutliveUserWritesInOrange) {
        TSpaceColorEnv env;
        for (auto color : {TColor::PRE_ORANGE, TColor::ORANGE}) {
            env.SetColor(color);
            UNIT_ASSERT_VALUES_EQUAL_C(env.Put(TDataKind::USER), NKikimrProto::OUT_OF_SPACE,
                TPDiskSpaceColor_Name(color));
            UNIT_ASSERT_VALUES_EQUAL_C(env.Put(TDataKind::SYSTEM), NKikimrProto::OK, TPDiskSpaceColor_Name(color));
        }
    }

    Y_UNIT_TEST(RedKeepsOnlyUnavoidableSystemWrites) {
        TSpaceColorEnv env;
        env.SetColor(TColor::RED);

        UNIT_ASSERT_VALUES_EQUAL(env.Put(TDataKind::USER), NKikimrProto::OUT_OF_SPACE);
        UNIT_ASSERT_VALUES_EQUAL(env.Put(TDataKind::SYSTEM), NKikimrProto::OUT_OF_SPACE);

        // IgnoreBlock marks the discovery writes a tablet issues while booting: a system tablet must
        // still be able to come up at RED, otherwise nobody can delete anything.
        UNIT_ASSERT_VALUES_EQUAL(env.Put(TDataKind::SYSTEM, true), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(env.Put(TDataKind::USER, true), NKikimrProto::OUT_OF_SPACE);
    }

    // A tablet reading its own log during boot asks for MustRestoreFirst, and DSProxy answers by
    // writing the parts it could not find. That write has to be admitted like the tablet's own
    // writes, or a system tablet cannot get up in a group that has run out of space.
    Y_UNIT_TEST(RestoreWriteInheritsTheKindOfTheRead) {
        TPartialBlobEnv env;
        for (const auto dataKind : {TDataKind::USER, TDataKind::SYSTEM}) {
            const auto kinds = env.RestoreWriteKinds(env.WriteDataPartsOnly(), dataKind);
            UNIT_ASSERT_C(!kinds.empty(), "the read restored nothing, so it proves nothing");
            for (const auto kind : kinds) {
                UNIT_ASSERT_EQUAL(kind, dataKind);
            }
        }
    }

    Y_UNIT_TEST(BlackStopsEverything) {
        TSpaceColorEnv env;
        env.SetColor(TColor::BLACK);

        UNIT_ASSERT_VALUES_EQUAL(env.Put(TDataKind::USER, true), NKikimrProto::OUT_OF_SPACE);
        UNIT_ASSERT_VALUES_EQUAL(env.Put(TDataKind::SYSTEM, true), NKikimrProto::OUT_OF_SPACE);
    }

}
