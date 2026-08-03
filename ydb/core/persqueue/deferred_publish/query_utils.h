#pragma once

#include <util/generic/maybe.h>
#include <util/generic/string.h>

#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NKikimr::NPQ::NDeferredPublish {

bool IsRegistryTableMissing(Ydb::StatusIds::StatusCode status);

bool IsAuthenticatedCallerSid(TStringBuf callerSid);

bool IsPublicationOwnedByCaller(TStringBuf callerSid, const TMaybe<TString>& createdBy);

} // namespace NKikimr::NPQ::NDeferredPublish
