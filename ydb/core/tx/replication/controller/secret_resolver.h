#pragma once

#include <ydb/core/base/defs.h>
#include <ydb/core/protos/replication.pb.h>

namespace NKikimr::NReplication::NController {

IActor* CreateSecretResolver(const TActorId& parent, ui64 rid,
    const TPathId& pathId, const TString& secretName, const ui64 cookie, const TString& database);

// A recreated schema object is not a new revision of the original secret.
bool IsNewSecretRevision(const TString& previous, const TString& next);
IActor* CreateSecretRefresh(const TActorId& parent, ui64 rid, const TPathId& pathId,
    const TString& database, const NKikimrReplication::TConnectionParams& params,
    const TString& failedRevision, TVector<TString> topics);

}
