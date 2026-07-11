#pragma once

#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NKikimr::NPQ::NDeferredPublish {

bool IsRegistryTableMissing(Ydb::StatusIds::StatusCode status);

} // namespace NKikimr::NPQ::NDeferredPublish
