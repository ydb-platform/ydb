#include "schemeshard__operation_streaming_query_common.h"
#include "schemeshard_info_types.h"
#include "schemeshard_path.h"

#include <ydb/core/base/path.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/services/workload_manager/common/helpers.h>

#include <util/generic/string.h>
#include <util/system/types.h>

#include <utility>

namespace NKikimr::NSchemeShard::NStreamingQuery {

THolder<NMetadata::NProvider::TEvTrackOperationCompletion> MakeStreamingOperationTrackerRequest(
    const TPath& path, const ui64 ssGeneration, const TStreamingQueryInfo& query)
{
    auto ev = MakeHolder<NMetadata::NProvider::TEvTrackOperationCompletion>();
    ev->SetTypeId("STREAMING_QUERY");
    ev->SetPathId(path.Base()->PathId);
    ev->SetRequestGeneration(ssGeneration);
    ev->SetObjectGeneration(query.AlterVersion);
    ev->SetOperationOwner(query.OperationOwnerActorId);
    ev->SetUserToken(query.OperationOwnerUserToken);

    auto database = path.GetDomainPathString();
    const auto domainKey = path.GetDomainKey();
    const auto resourcesDomainKey = path.DomainInfo()->GetResourcesDomainId();
    ev->SetDatabaseId(NWorkloadManager::CreateDatabaseId(database, resourcesDomainKey != domainKey, domainKey));
    {
        std::pair<TString, TString> result;
        TString error;
        Y_VALIDATE(TrySplitPathByDb(path.PathString(), database, result, error), "Failed to split streaming query path '" << path.PathString() << "' by database '" << database << "': " << error);
        ev->SetObjectId(std::move(result.second));
    }

    ev->SetDatabase(std::move(database));
    return ev;
}

} // namespace NKikimr::NSchemeShard::NStreamingQuery
