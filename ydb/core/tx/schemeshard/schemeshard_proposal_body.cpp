#include "schemeshard_proposal_body.h"

namespace NKikimr::NSchemeShard::NProposalBody {

void SerializePiece(TString& out, NKikimrTxDataShard::TFlatSchemeTransaction& t) {
    out.clear();
    Y_PROTOBUF_SUPPRESS_NODISCARD t.SerializeToString(&out);
}

THolder<TEvDataShard::TEvProposeTransaction> MakeDataShardProposal(
    ui64 tabletId,
    const TActorId& selfId,
    TTxId txId,
    const NKikimrSubDomains::TProcessingParams& processingParams
) {
    return MakeHolder<TEvDataShard::TEvProposeTransaction>(
        NKikimrTxDataShard::TX_KIND_SCHEME, tabletId, selfId,
        ui64(txId), TStringBuf(""), processingParams
    );
}

} // namespace NKikimr::NSchemeShard::NProposalBody
