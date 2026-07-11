#include "query_utils.h"

namespace NKikimr::NPQ::NDeferredPublish {

bool IsRegistryTableMissing(Ydb::StatusIds::StatusCode status) {
    return status == Ydb::StatusIds::SCHEME_ERROR;
}

} // namespace NKikimr::NPQ::NDeferredPublish
