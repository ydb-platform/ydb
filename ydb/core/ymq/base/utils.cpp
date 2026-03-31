#include "utils.h"

#include <ydb/core/persqueue/public/mlp/mlp.h>


namespace NKikimr::NSQS {

TString ToMessageId(const NPQ::NMLP::TMessageId& messageId) {
    return TStringBuilder() << "m-" << messageId.PartitionId << "-" << messageId.Offset;
}

} // namespace NKikimr::NSQS
