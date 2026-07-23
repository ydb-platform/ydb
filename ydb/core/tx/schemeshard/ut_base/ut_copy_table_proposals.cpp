#include <ydb/core/tx/schemeshard/schemeshard__operation_copy_table.h> // for TShardProposal and TShardProposalInputs

#include <library/cpp/testing/unittest/registar.h>

namespace {

using namespace NKikimr;
using namespace NSchemeShard;

TVector<TTableShardInfo> MakeShards(ui32 n, ui64 ownerId = 1) {
    TVector<TTableShardInfo> v;
    v.reserve(n);
    for (ui32 i = 0; i < n; ++i) {
        TString range = (i + 1 < n) ? TString(1, char(i + 1)) : TString{};
        v.emplace_back(TShardIdx(ownerId, i), range);
    }
    return v;
}

struct TTestInputs {
    TTableInfo::TPtr SrcTable;
    TTableInfo::TPtr DstTable;
    THashMap<TShardIdx, TTabletId> ShardToTabletMap;
    NKikimrSchemeOp::TTableDescription EmptyChildren;
    TVector<TPathId> StreamsToDrop;
    NKikimrTxDataShard::TCreateCdcStreamNotice CreateCdcNotice;
    NKikimrSubDomains::TProcessingParams ProcessingParams;

    NCopyTable::TShardProposalInputs MakeInputs(
        bool useIncrementalBackup = false,
        ui64 coordVersion = 0,
        TPathId cdcPathId = TPathId())
    {
        return NCopyTable::TShardProposalInputs{
            .SrcTable = *SrcTable,
            .DstTable = *DstTable,
            .ShardToTablet = [this](TShardIdx idx) -> TTabletId {
                return ShardToTabletMap.at(idx);
            },
            .SourcePathId = TPathId(1, 10),
            .TargetPathId = TPathId(1, 20),
            .CdcPathId = cdcPathId,
            .DstSubDomainPathId = 77,
            .DstProcessingParams = ProcessingParams,
            .SelfTabletId = 72057594046678944ull,
            .SelfId = TActorId(),
            .SeqNo = TMessageSeqNo{3, 5},
            .DstSchemaVersion = 1,
            .TxId = TTxId(42),
            .DstPath = "/Root/dst",
            .DstName = "dst",
            .DstChildrenTemplate = EmptyChildren,
            .UseIncrementalBackup = useIncrementalBackup,
            .StreamsToDrop = StreamsToDrop,
            .CoordVersion = coordVersion,
            .CreateCdcNotice = CreateCdcNotice,
        };
    }
};

// A fabricated table matching what a real SchemeShard feeds into the builders:
// key column + value columns, KeyColumnIds maintained, optional per-shard config patches.
TTableInfo::TPtr MakeTestTable(ui32 numPartitions, bool withPerShardPatch) {
    auto info = MakeIntrusive<TTableInfo>();
    info->Columns.emplace(1, TTableInfo::TColumn("key", 1, NScheme::NTypeIds::Int64, "", true));
    info->Columns[1].KeyOrder = 0;
    info->KeyColumnIds = {1};
    for (ui32 c = 0; c < 3; ++c) {
        const TString colName = TStringBuilder() << "col_" << c;
        info->Columns.emplace(2 + c, TTableInfo::TColumn(colName, 2 + c, NScheme::NTypeIds::Uint64, "", false));
    }
    info->SetPartitioning(MakeShards(numPartitions));

    if (withPerShardPatch) {
        for (ui32 i = 0; i < numPartitions; ++i) {
            auto& patch = info->PerShardPartitionConfig[TShardIdx(2, i)];
            auto* family = patch.AddColumnFamilies();
            family->SetId(0);
            family->SetRoom(i % 2);
            auto* room = patch.AddStorageRooms();
            room->SetRoomId(i % 2);
        }
    }
    return info;
}

TTestInputs MakeTestInputs(ui32 numPartitions, bool withPerShardPatch) {
    TTestInputs in;
    in.SrcTable = MakeTestTable(numPartitions, false);
    in.DstTable = MakeTestTable(numPartitions, withPerShardPatch);
    for (ui32 i = 0; i < numPartitions; ++i) {
        in.ShardToTabletMap[TShardIdx(1, i)] = TTabletId(1000 + i);  // src
        in.ShardToTabletMap[TShardIdx(2, i)] = TTabletId(2000 + i);  // dst
    }
    return in;
}

// ---------------------------------------------------------------------------
// Reference implementation: the straightforward full-build path. Assembles the
// complete TFlatSchemeTransaction per proposal and serializes it in one go.
// BuildConfigurePartsProposals must produce semantically identical bodies
// (however it assembles them internally — full build or prefix/delta concat).
// ---------------------------------------------------------------------------

void RefFillSeqNo(NKikimrTxDataShard::TFlatSchemeTransaction& tx, TMessageSeqNo seqNo) {
    tx.MutableSeqNo()->SetGeneration(seqNo.Generation);
    tx.MutableSeqNo()->SetRound(seqNo.Round);
}

THolder<TEvDataShard::TEvProposeTransaction> RefMakeProposal(
    ui64 tabletId, const TActorId& selfId, TTxId txId,
    const TString& body, const NKikimrSubDomains::TProcessingParams& params)
{
    return MakeHolder<TEvDataShard::TEvProposeTransaction>(
        NKikimrTxDataShard::TX_KIND_SCHEME, tabletId, selfId,
        ui64(txId), body, params);
}

struct TRefProposal {
    TTabletId TabletId;
    TShardIdx ShardIdx;
    TString Body;  // serialized TFlatSchemeTransaction
};

TVector<TRefProposal> RefBuildProposals(const NCopyTable::TShardProposalInputs& in) {
    const auto& srcPartitions = in.SrcTable.GetPartitions();
    const auto& dstPartitions = in.DstTable.GetPartitions();

    TVector<TRefProposal> proposals;
    proposals.reserve(dstPartitions.size() * 2);

    for (ui32 i = 0; i < dstPartitions.size(); ++i) {
        const TShardIdx srcShardIdx = srcPartitions[i]->ShardIdx;
        const TTabletId srcDatashardId = in.ShardToTablet(srcShardIdx);
        const TShardIdx dstShardIdx = dstPartitions[i]->ShardIdx;
        const TTabletId dstDatashardId = in.ShardToTablet(dstShardIdx);

        // --- dst proposal: CreateTable + ReceiveSnapshot ---
        {
            NKikimrTxDataShard::TFlatSchemeTransaction newShardTx;
            RefFillSeqNo(newShardTx, in.SeqNo);

            auto* createTable = newShardTx.MutableCreateTable();
            NCopyTable::FillTableDescription(
                in.DstTable, i, in.DstSchemaVersion,
                in.DstPath, in.DstName, in.TargetPathId, createTable);
            if (in.DstChildrenTemplate.TableIndexesSize()) {
                createTable->MutableTableIndexes()->CopyFrom(in.DstChildrenTemplate.GetTableIndexes());
            }
            if (in.DstChildrenTemplate.CdcStreamsSize()) {
                createTable->MutableCdcStreams()->CopyFrom(in.DstChildrenTemplate.GetCdcStreams());
            }
            if (in.DstChildrenTemplate.SequencesSize()) {
                createTable->MutableSequences()->CopyFrom(in.DstChildrenTemplate.GetSequences());
            }

            newShardTx.MutableReceiveSnapshot()->SetTableId_Deprecated(in.TargetPathId.LocalPathId);
            newShardTx.MutableReceiveSnapshot()->MutableTableId()->SetOwnerId(in.TargetPathId.OwnerId);
            newShardTx.MutableReceiveSnapshot()->MutableTableId()->SetTableId(in.TargetPathId.LocalPathId);
            newShardTx.MutableReceiveSnapshot()->AddReceiveFrom()->SetShard(ui64(srcDatashardId));

            auto ev = RefMakeProposal(in.SelfTabletId, in.SelfId, in.TxId,
                newShardTx.SerializeAsString(), in.DstProcessingParams);
            if (in.DstSubDomainPathId) {
                ev->Record.SetSubDomainPathId(in.DstSubDomainPathId);
            }
            proposals.push_back({dstDatashardId, dstShardIdx, ev->Record.GetTxBody()});
        }

        // --- src proposal: SendSnapshot / CreateIncrementalBackupSrc ---
        {
            NKikimrTxDataShard::TFlatSchemeTransaction oldShardTx;
            RefFillSeqNo(oldShardTx, in.SeqNo);

            if (in.UseIncrementalBackup) {
                auto& combined = *oldShardTx.MutableCreateIncrementalBackupSrc();
                auto& snapshot = *combined.MutableSendSnapshot();
                snapshot.SetTableId_Deprecated(in.SourcePathId.LocalPathId);
                snapshot.MutableTableId()->SetOwnerId(in.SourcePathId.OwnerId);
                snapshot.MutableTableId()->SetTableId(in.SourcePathId.LocalPathId);
                snapshot.AddSendTo()->SetShard(ui64(dstDatashardId));

                const bool hasDrop = !in.StreamsToDrop.empty();
                const bool hasCreate = (in.CdcPathId != InvalidPathId);

                if (hasDrop) {
                    auto& dropNotice = *combined.MutableDropCdcStreamNotice();
                    in.SourcePathId.ToProto(dropNotice.MutablePathId());
                    dropNotice.SetTableSchemaVersion(in.CoordVersion);
                    for (const auto& id : in.StreamsToDrop) {
                        id.ToProto(dropNotice.AddStreamPathId());
                    }
                }
                if (hasCreate) {
                    *combined.MutableCreateCdcStreamNotice() = in.CreateCdcNotice;
                    combined.MutableCreateCdcStreamNotice()->SetTableSchemaVersion(in.CoordVersion);
                }
            } else {
                auto& snapshot = *oldShardTx.MutableSendSnapshot();
                snapshot.SetTableId_Deprecated(in.SourcePathId.LocalPathId);
                snapshot.MutableTableId()->SetOwnerId(in.SourcePathId.OwnerId);
                snapshot.MutableTableId()->SetTableId(in.SourcePathId.LocalPathId);
                snapshot.AddSendTo()->SetShard(ui64(dstDatashardId));
                oldShardTx.SetReadOnly(true);
            }

            auto ev = RefMakeProposal(in.SelfTabletId, in.SelfId, in.TxId,
                oldShardTx.SerializeAsString(), in.DstProcessingParams);
            proposals.push_back({srcDatashardId, srcShardIdx, ev->Record.GetTxBody()});
        }
    }

    return proposals;
}

// Canonical comparison: parse both bodies and compare the canonical re-serialization,
// so differences in field ordering / concatenation do not matter, only semantics.
void AssertProposalsEquivalent(
    const TVector<NCopyTable::TShardProposal>& actual,
    const TVector<TRefProposal>& expected)
{
    UNIT_ASSERT_VALUES_EQUAL(actual.size(), expected.size());
    for (size_t i = 0; i < actual.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(ui64(actual[i].TabletId), ui64(expected[i].TabletId));
        UNIT_ASSERT_VALUES_EQUAL(actual[i].ShardIdx, expected[i].ShardIdx);

        NKikimrTxDataShard::TFlatSchemeTransaction actualTx, expectedTx;
        UNIT_ASSERT(actualTx.ParseFromString(actual[i].Event->Record.GetTxBody()));
        UNIT_ASSERT(expectedTx.ParseFromString(expected[i].Body));
        UNIT_ASSERT_VALUES_EQUAL(
            actualTx.SerializeAsString(),
            expectedTx.SerializeAsString());

        // Event envelope fields must match too.
        UNIT_ASSERT_VALUES_EQUAL(
            ui64(actual[i].Event->Record.GetTxId()), ui64(42));
        UNIT_ASSERT_VALUES_EQUAL(
            actual[i].Event->Record.GetSchemeShardId(), 72057594046678944ull);
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TCopyTableProposalsEquivalence) {

    Y_UNIT_TEST(Basic) {
        for (ui32 n : {1u, 4u, 16u}) {
            for (bool withPatch : {false, true}) {
                auto in = MakeTestInputs(n, withPatch);
                auto inputs = in.MakeInputs();
                auto actual = NCopyTable::BuildConfigurePartsProposals(inputs);
                auto expected = RefBuildProposals(inputs);
                AssertProposalsEquivalent(actual, expected);
            }
        }
    }

    Y_UNIT_TEST(BackupRestoreFlags) {
        auto in = MakeTestInputs(4, false);
        in.DstTable->IsBackup = true;
        in.SrcTable->IsRestore = true;
        auto inputs = in.MakeInputs();
        AssertProposalsEquivalent(
            NCopyTable::BuildConfigurePartsProposals(inputs),
            RefBuildProposals(inputs));
    }

    Y_UNIT_TEST(ReplicationConfig) {
        auto in = MakeTestInputs(4, false);
        in.DstTable->MutableReplicationConfig().SetMode(
            NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY);
        auto inputs = in.MakeInputs();
        AssertProposalsEquivalent(
            NCopyTable::BuildConfigurePartsProposals(inputs),
            RefBuildProposals(inputs));
    }

    Y_UNIT_TEST(IncrementalBackupConfig) {
        auto in = MakeTestInputs(4, false);
        in.DstTable->MutableIncrementalBackupConfig().SetMode(
            NKikimrSchemeOp::TTableIncrementalBackupConfig::RESTORE_MODE_INCREMENTAL_BACKUP);
        auto inputs = in.MakeInputs();
        AssertProposalsEquivalent(
            NCopyTable::BuildConfigurePartsProposals(inputs),
            RefBuildProposals(inputs));
    }

    Y_UNIT_TEST(IncrementalBackupSrcPath) {
        // Drop + create notices: exercises the CreateIncrementalBackupSrc branch.
        auto in = MakeTestInputs(4, true);
        in.StreamsToDrop = {TPathId(1, 11), TPathId(1, 12)};
        in.CreateCdcNotice.MutablePathId()->SetOwnerId(1);
        in.CreateCdcNotice.MutablePathId()->SetLocalId(13);
        auto inputs = in.MakeInputs(
            /*useIncrementalBackup=*/true,
            /*coordVersion=*/99,
            /*cdcPathId=*/TPathId(1, 13));
        AssertProposalsEquivalent(
            NCopyTable::BuildConfigurePartsProposals(inputs),
            RefBuildProposals(inputs));
    }

    Y_UNIT_TEST(ChildrenTemplate) {
        auto in = MakeTestInputs(4, false);
        // Non-empty children template: indexes/cdc/sequences merge into CreateTable.
        in.EmptyChildren.AddTableIndexes()->SetName("idx");
        in.EmptyChildren.AddCdcStreams()->SetName("stream");
        in.EmptyChildren.AddSequences()->SetName("seq");
        auto inputs = in.MakeInputs();
        AssertProposalsEquivalent(
            NCopyTable::BuildConfigurePartsProposals(inputs),
            RefBuildProposals(inputs));
    }

} // Y_UNIT_TEST_SUITE(TCopyTableProposalsEquivalence)
