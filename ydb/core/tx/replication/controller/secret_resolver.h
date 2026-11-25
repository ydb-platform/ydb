#pragma once

#include <ydb/core/base/defs.h>

#include <optional>

namespace NKikimrReplication {
class TConnectionParams;
}

namespace NKikimr::NReplication::NController {

IActor* CreateSecretResolver(const TActorId& parent, ui64 rid,
    const TPathId& pathId, const TString& secretName, const ui64 cookie, const TString& database);

// Legacy metadata supplies a value without a per-secret revision.
inline constexpr char LegacySecretRecoveryMarker[] = "legacy";

// Older controllers reject this tag as an unknown secret revision and retain Error.
inline constexpr char MissingIndexStreamRecoveryMarker[] = "missing-index-stream";

// Minimum time after the authentication failure before probing this revision.
std::optional<TDuration> GetSecretRefreshDelay(TStringBuf failed, TStringBuf resolved);

struct TSecretProbeTopic {
    struct TIndex {
        ui64 TargetId;
        TString TablePath;
        TString Name;
    };

    TString Path;
    std::optional<TIndex> SchemaManagedIndex;
};

IActor* CreateSecretRefresh(const TActorId& parent, ui64 rid, const TPathId& pathId,
    const TString& database, const NKikimrReplication::TConnectionParams& params,
    const TString& failedRevision, TVector<TSecretProbeTopic> topics);

}
