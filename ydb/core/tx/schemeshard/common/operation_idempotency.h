#pragma once

#include <ydb/core/protos/schemeshard/operations.pb.h>

#include <util/generic/strbuf.h>

namespace NKikimrSchemeOp {
class TModifyScheme;
}

namespace NKqpProto {
class TKqpSchemeOperation;
}

namespace NKikimr::NSchemeShard {

bool SupportsOperationIdempotency(NKikimrSchemeOp::EOperationType operationType);
bool SupportsSqlOperationIdempotency(TStringBuf writeMode);

// Return the supported operation payload, or nullptr for unsupported or
// inconsistent physical operations.
const NKikimrSchemeOp::TModifyScheme* GetSchemeOperationForIdempotency(
    const NKqpProto::TKqpSchemeOperation& operation);

} // namespace NKikimr::NSchemeShard
