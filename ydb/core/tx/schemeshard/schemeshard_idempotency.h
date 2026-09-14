#pragma once

#include "schemeshard_identificators.h"

#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/maybe.h>
#include <utility>

namespace Ydb::Operations {
class OperationParams;
}

namespace NKikimr::NSchemeShard {

inline bool IsValidOperationUid(TStringBuf key) {
    return !key.empty() && key.size() <= 128;
}

TString GetUid(const Ydb::Operations::OperationParams& operationParams);

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

// A UID belongs to an operation type on this SchemeShard tablet.
using TBackupOperationUidKey = std::pair<ui32, TString>;

struct TBackupOperationReplay {
    ui64 OperationId = 0;
    TString OriginalDdl;
    TString UserSID;
};

} // namespace NKikimr::NSchemeShard
