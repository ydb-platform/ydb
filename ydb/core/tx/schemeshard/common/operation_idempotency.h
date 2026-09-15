#pragma once

#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/scheme/scheme_pathid.h>

#include <util/generic/maybe.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>

#include <functional>
#include <utility>

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

// Only the identity fields requested by the operation are compared.
EUidReplayMatch CompareOperationUid(const TOperationUidIdentity& stored, const TOperationUidIdentity& requested);

// The kind preserves the separate UID namespaces of the existing operations.
using TOperationUidKey = std::pair<Ydb::TOperationId_EKind, TString>;

struct TOperationUidRecord {
    ui64 OperationId = 0;
    TMaybe<TPathId> DomainPathId;
    TString UserSID;
    TString RequestBody;
};

// This object is local to one admission transaction. Callbacks run synchronously;
// operation handlers retain ownership of their records, checks, and persistence.
class TOperationUidAdmission {
public:
    enum class EDuplicatePolicy { Replay, Reject };
    enum class EDecision { Proceed, Replay, AlreadyExists, OwnerMismatch, DomainMismatch, RequestMismatch };

    using TLookup = std::function<TMaybe<TOperationUidRecord>(const TOperationUidKey&)>;
    using TCheck = std::function<EUidReplayMatch(const TOperationUidRecord&)>;

    // Empty legacy UIDs disable deduplication. Protocol-specific validation
    // (including rejecting explicitly empty SQL UIDs) belongs to the caller.
    static TOperationUidAdmission Prepare(const TOperationUidKey& key,
        EDuplicatePolicy policy, const TLookup& lookup, const TCheck& check = {});

    EDecision GetDecision() const { return Decision; }
    ui64 GetOperationId() const { return OperationId; }

    // Invoke the operation's persistence/binding callback only for newly
    // admitted work. The callback participates in the caller's local transaction.
    bool Commit(bool admitted, const std::function<void()>& persist);

private:
    EDecision Decision = EDecision::Proceed;
    ui64 OperationId = 0;
    bool Committed = false;
};

TMaybe<Ydb::TOperationId_EKind> GetOperationUidKind(NKikimrSchemeOp::EOperationType operationType);

// Capabilities of keyed TModifyScheme submissions and their SQL/KQP forms.
// Legacy RPC admission paths use the UID helpers above.
bool SupportsOperationIdempotency(NKikimrSchemeOp::EOperationType operationType);
bool SupportsSqlOperationIdempotency(TStringBuf writeMode);

// Return the supported operation payload, or nullptr for unsupported or
// inconsistent physical operations.
const NKikimrSchemeOp::TModifyScheme* GetSchemeOperationForIdempotency(
    const NKqpProto::TKqpSchemeOperation& operation);

} // namespace NKikimr::NSchemeShard
