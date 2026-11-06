#include "target_secret.h"

#include <ydb/core/base/path.h>
#include <ydb/core/protos/replication.pb.h>

namespace NKikimr::NReplication::NController {

TString TTargetSecret::BuildStreamPath() const {
    const auto* replication = GetReplication();
    const auto& sourceDatabase = replication->GetConfig().GetSrcConnectionParams().GetDatabase();
    const auto& database = sourceDatabase.empty() ? replication->GetDatabase() : sourceDatabase;
    return CanonizePath(ChildPath(SplitPath(database), {"secretImpl", GetStreamName()}));
}

void TTargetSecret::ProgressDestination(const TActorContext& ctx) {
    // TSecretCopier creates the destination during initial copy. Only its
    // removal belongs to the common destination lifecycle.
    if (GetReplication()->GetState() == TReplication::EState::Removing
        && GetDstState() == TReplication::EDstState::Removing)
    {
        TTargetBase::Progress(ctx);
    }
}

}
