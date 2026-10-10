#pragma once

#include "schemeshard_identificators.h"  // for TTxId

#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/message_seqno.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/protos/subdomains.pb.h>

#include <util/generic/ptr.h>

// Shared building blocks for assembling scheme-tx proposal bodies by protobuf
// wire-format concatenation: a precomputed invariant "common" part (serialized
// once per propose round) plus a small per-shard "delta" piece. Concatenating
// serialized pieces of the same message type is equivalent to merging them
// under protobuf parse semantics (repeated fields concatenate, scalars
// last-win, message fields merge), so the assembled body is semantically
// identical to a full build.
//
// Rules the callers must follow (see plans/proposal_building_generalization.md):
//  - a repeated field must live wholly in the common piece or wholly in the
//    delta, never in both (repeated fields concatenate, they do not override);
//  - a message field that must be "last wins" (e.g. PartitionConfig) must be
//    emitted by the delta only (a duplicated message field merges on parse);
//  - a oneof must live wholly in the common piece.
namespace NKikimr::NSchemeShard::NProposalBody {

// Serializes `t` (expected to carry exactly one top-level field) into `out`,
// reusing the string's capacity across calls.
void SerializePiece(TString& out, NKikimrTxDataShard::TFlatSchemeTransaction& t);

// Creates the proposal event with an empty body; the caller appends the
// serialized pieces directly into Record.MutableTxBody(), avoiding an
// intermediate serialization buffer and a copy.
THolder<TEvDataShard::TEvProposeTransaction> MakeDataShardProposal(
    ui64 tabletId,
    const TActorId& selfId,
    TTxId txId,
    const NKikimrSubDomains::TProcessingParams& processingParams);

} // namespace NKikimr::NSchemeShard::NProposalBody
