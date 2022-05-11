#include "ydb_control_plane_storage_impl.h"

#include <cstdint>

#include <util/datetime/base.h>
#include <util/generic/yexception.h>
#include <util/string/join.h>

#include <ydb/core/yq/libs/common/entity_id.h>
#include <ydb/core/yq/libs/control_plane_storage/events/events.h>
#include <ydb/core/yq/libs/control_plane_storage/schema.h>
#include <ydb/core/yq/libs/db_schema/db_schema.h>

#include <ydb/public/api/protos/yq.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_value/value.h>

#include <util/digest/multi.h>

namespace NYq {

void TYdbControlPlaneStorageActor::Handle(TEvQuotaService::TQuotaUsageRequest::TPtr& ev)
{
    auto& request = *ev->Get();
    Send(ev->Sender, new TEvQuotaService::TQuotaUsageResponse(request.SubjectType, request.SubjectId, request.MetricName, 0));
}

} // NYq
