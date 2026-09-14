#pragma once

#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/scheme/scheme_pathid.h>

#include <util/generic/maybe.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace Ydb {
enum TOperationId_EKind : int;
}

namespace Ydb::Operations {
class OperationParams;
}

namespace NKikimrSchemeOp {
class TModifyScheme;
}

namespace NKqpProto {
class TKqpSchemeOperation;
}

namespace NKikimr::NSchemeShard {

inline bool IsValidOperationUid(TStringBuf key) {
    return !key.empty() && key.size() <= 128;
}

// UID availability is independent of SQL support and of duplicate handling.
// Some legacy operations reject duplicates instead of returning an existing ID.
bool SupportsOperationUid(Ydb::TOperationId_EKind kind);
bool SupportsSqlOperationIdempotency(Ydb::TOperationId_EKind kind);

TString GetUid(const Ydb::Operations::OperationParams& operationParams);
// Admission handlers pass their fixed operation kind; an unregistered kind is
// a programming error. Keep the untyped extractor for diagnostics such as audit.
TString GetUid(Ydb::TOperationId_EKind kind, const Ydb::Operations::OperationParams& operationParams);

// Each operation type has an independent UID index; UIDs live with their operation records.
template <typename TIndex, typename TKey>
const typename TIndex::mapped_type* FindOperationByUid(const TIndex& index, const TKey& key) {
    const auto it = index.find(key);
    return it == index.end() ? nullptr : &it->second;
}

struct TOperationUidIdentity {
    TMaybe<TPathId> DomainPathId;
    TMaybe<TStringBuf> UserSID;
    TMaybe<TStringBuf> RequestBody;
};

enum class EUidReplayMatch {
    Match,
    OwnerMismatch,
    DomainMismatch,
    RequestMismatch,
};

// Import/export compare domains; backup/restore SQL compares owner and DDL.
EUidReplayMatch CompareOperationUid(const TOperationUidIdentity& stored, const TOperationUidIdentity& requested);

// Capabilities of keyed TModifyScheme submissions and their SQL/KQP forms.
// Legacy RPC admission paths use the UID helpers above.
bool SupportsOperationIdempotency(NKikimrSchemeOp::EOperationType operationType);
bool SupportsSqlOperationIdempotency(TStringBuf writeMode);

// Return the supported operation payload, or nullptr for unsupported or
// inconsistent physical operations.
const NKikimrSchemeOp::TModifyScheme* GetSchemeOperationForIdempotency(
    const NKqpProto::TKqpSchemeOperation& operation);

} // namespace NKikimr::NSchemeShard
