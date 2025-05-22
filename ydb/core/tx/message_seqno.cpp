#include "message_seqno.h"
#include <ydb/core/protos/tx.pb.h>

namespace NKikimr {

TString TMessageSeqNo::SerializeToString() const {
    return SerializeToProto().SerializeAsString();
}

TConclusionStatus TMessageSeqNo::DeserializeFromString(const TString& data) {
    NKikimrTx::TMessageSeqNo proto;
    if (!proto.ParseFromArray(data.data(), data.size())) {
        return TConclusionStatus::Fail("cannot parse string as proto");
    }
    return DeserializeFromProto(proto);
}

NKikimrTx::TMessageSeqNo TMessageSeqNo::SerializeToProto() const {
    NKikimrTx::TMessageSeqNo result;
    result.SetGeneration(Generation);
    result.SetRound(Round);
    return result;
}

TConclusionStatus TMessageSeqNo::DeserializeFromProto(const NKikimrTx::TMessageSeqNo& proto) {
    Generation = proto.GetGeneration();
    Round = proto.GetRound();
    return TConclusionStatus::Success();
}

}
