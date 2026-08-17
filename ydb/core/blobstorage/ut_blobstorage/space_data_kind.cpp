#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_util_space_color.h>
#include <ydb/core/base/blobstorage_data_kind.h>

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

    Y_UNIT_TEST(BlackStopsEverything) {
        TSpaceColorEnv env;
        env.SetColor(TColor::BLACK);

        UNIT_ASSERT_VALUES_EQUAL(env.Put(TDataKind::USER, true), NKikimrProto::OUT_OF_SPACE);
        UNIT_ASSERT_VALUES_EQUAL(env.Put(TDataKind::SYSTEM, true), NKikimrProto::OUT_OF_SPACE);
    }

}
