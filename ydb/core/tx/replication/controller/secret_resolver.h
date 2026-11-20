#pragma once

#include <ydb/core/base/defs.h>
#include <ydb/core/protos/replication.pb.h>

#include <optional>

namespace NKikimr::NReplication::NController {

IActor* CreateSecretResolver(const TActorId& parent, ui64 rid,
    const TPathId& pathId, const TString& secretName, const ui64 cookie, const TString& database);

// Legacy metadata supplies a value without a per-secret revision.
inline constexpr char LegacySecretRecoveryMarker[] = "legacy";

bool CanRefreshSecret(TStringBuf failed, TStringBuf resolved);

struct TSecretProbeTopic {
    struct TIndex {
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
