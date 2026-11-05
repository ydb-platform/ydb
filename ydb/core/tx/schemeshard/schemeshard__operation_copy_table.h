#pragma once

#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/message_seqno.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/protos/subdomains.pb.h>

#include <util/generic/vector.h>
#include <util/generic/ptr.h>

#include <functional>

// Free (SchemeShard-independent) building blocks for the CopyTable ConfigureParts phase.
// They take every input explicitly so they can run without a live TSchemeShard/tablet/actor
// (see the tablet-free benchmark and the equivalence ut). The TSchemeShard members of the
// same name gather the data from `this` and forward here. The definitions live in
// schemeshard__operation_copy_table.cpp.
namespace NKikimr::NSchemeShard::NCopyTable {

// One built datashard proposal together with its pipe routing (destination tablet + shard).
struct TShardProposal {
    TTabletId TabletId;
    TShardIdx ShardIdx;
    THolder<TEvDataShard::TEvProposeTransaction> Event;
};

// Plain-data inputs for the CopyTable ConfigureParts phase, extracted from the SchemeShard so
// that proposal building runs without a live tablet/actor/txc. Reference members must outlive
// the BuildConfigurePartsProposals call.
struct TShardProposalInputs {
    const TTableInfo& SrcTable;
    TTableInfo& DstTable;  // non-const: FillTableDescription fills the description cache
    std::function<TTabletId(TShardIdx)> ShardToTablet;

    TPathId SourcePathId;
    TPathId TargetPathId;
    TPathId CdcPathId;

    ui64 DstSubDomainPathId = 0;
    const NKikimrSubDomains::TProcessingParams& DstProcessingParams;

    ui64 SelfTabletId = 0;
    TActorId SelfId;
    TMessageSeqNo SeqNo;
    ui64 DstSchemaVersion = 0;
    TTxId TxId;

    TString DstPath;
    TString DstName;
    // Pre-resolved child index/cdc/sequence descriptions to merge into the dst CreateTable
    // (empty when the table has no children, e.g. in the tablet-free benchmark).
    const NKikimrSchemeOp::TTableDescription& DstChildrenTemplate;

    bool UseIncrementalBackup = false;
    const TVector<TPathId>& StreamsToDrop;
    ui64 CoordVersion = 0;
    const NKikimrTxDataShard::TCreateCdcStreamNotice& CreateCdcNotice;
};

void ApplyPartitionConfigStoragePatch(
    NKikimrSchemeOp::TPartitionConfig& config,
    const NKikimrSchemeOp::TPartitionConfig& patch);

void FillTableDescription(
    TTableInfo& tableInfo, ui32 partitionIdx, ui64 schemaVersion,
    const TString& path, const TString& name, TPathId pathId,
    NKikimrSchemeOp::TTableDescription* tableDescr);

TVector<TShardProposal> BuildConfigurePartsProposals(const TShardProposalInputs& in);

} // namespace NKikimr::NSchemeShard::NCopyTable
