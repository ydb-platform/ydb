#include "query_utils.h"

#include <ydb/library/aclib/aclib.h>

namespace NKikimr::NPQ::NDeferredPublish {

bool IsRegistryTableMissing(Ydb::StatusIds::StatusCode status) {
    return status == Ydb::StatusIds::SCHEME_ERROR;
}

bool IsAuthenticatedCallerSid(TStringBuf callerSid) {
    return !callerSid.empty() && callerSid != BUILTIN_ACL_NO_USER_SID;
}

bool IsPublicationOwnedByCaller(TStringBuf callerSid, const TMaybe<TString>& createdBy) {
    return createdBy.Defined() && *createdBy == callerSid;
}

} // namespace NKikimr::NPQ::NDeferredPublish
