#pragma once

#include <ydb/public/api/protos/persqueue_error_codes_v1.pb.h> 

#include <ydb/library/aclib/aclib.h> 
#include <ydb/core/scheme/scheme_tabledefs.h> 
#include <ydb/core/base/counters.h> 
#include <ydb/core/tx/scheme_cache/scheme_cache.h> 

namespace NKikimr::NGRpcProxy::V1 {

class TAclWrapper {
public:
    TAclWrapper(THolder<NACLib::TSecurityObject>);
    TAclWrapper(TIntrusivePtr<TSecurityObject>);

    bool CheckAccess(NACLib::EAccessRights, const NACLib::TUserToken& userToken);

private:
    THolder<NACLib::TSecurityObject> AclOldSchemeCache;
    TIntrusivePtr<TSecurityObject> AclNewSchemeCache;
};

struct TProcessingResult {
    Ydb::PersQueue::ErrorCode::ErrorCode ErrorCode;
    TString Reason;
    bool IsFatal = false;
};

TProcessingResult ProcessMetaCacheTopicResponse(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry);

} //namespace NKikimr::NGRpcProxy::V1
