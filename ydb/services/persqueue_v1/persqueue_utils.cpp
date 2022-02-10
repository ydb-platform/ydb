#include "persqueue_utils.h"

#include <ydb/core/base/path.h> 

namespace NKikimr::NGRpcProxy::V1 {

TAclWrapper::TAclWrapper(THolder<NACLib::TSecurityObject> acl)
    : AclOldSchemeCache(std::move(acl))
{
    Y_VERIFY(AclOldSchemeCache);
}

TAclWrapper::TAclWrapper(TIntrusivePtr<TSecurityObject> acl)
    : AclNewSchemeCache(std::move(acl))
{
    Y_VERIFY(AclNewSchemeCache);
}

bool TAclWrapper::CheckAccess(NACLib::EAccessRights rights, const NACLib::TUserToken& userToken) {
    if (AclOldSchemeCache) {
        return AclOldSchemeCache->CheckAccess(rights, userToken);
    } else {
        return AclNewSchemeCache->CheckAccess(rights, userToken);
    }
}

using namespace NSchemeCache;

TProcessingResult ProcessMetaCacheTopicResponse(const TSchemeCacheNavigate::TEntry& entry) {
    auto fullPath = JoinPath(entry.Path);
    auto& topicName = entry.Path.back();
    switch (entry.Status) {
        case TSchemeCacheNavigate::EStatus::RootUnknown : {
            return TProcessingResult {
                    Ydb::PersQueue::ErrorCode::ErrorCode::BAD_REQUEST,
                    Sprintf("path '%s' has unknown/invalid root prefix '%s', Marker# PQ14",
                            fullPath.c_str(), entry.Path[0].c_str()),
                    true
            };
        }
        case TSchemeCacheNavigate::EStatus::PathErrorUnknown: {
            return TProcessingResult {
                    Ydb::PersQueue::ErrorCode::ErrorCode::UNKNOWN_TOPIC,
                    Sprintf("no path '%s', Marker# PQ15", fullPath.c_str()),
                    true
            };
        }
        case TSchemeCacheNavigate::EStatus::Ok:
            break;
        default: {
            return TProcessingResult {
                    Ydb::PersQueue::ErrorCode::ErrorCode::ERROR,
                    Sprintf("topic '%s' describe error, Status# %s, Marker# PQ1",
                            topicName.c_str(), ToString(entry.Status).c_str()),
                    true
            };
        }
    }

    if (entry.Kind != TSchemeCacheNavigate::KindTopic) {
        return TProcessingResult {
                Ydb::PersQueue::ErrorCode::ErrorCode::UNKNOWN_TOPIC,
                Sprintf("item '%s' is not a topic, Marker# PQ13", fullPath.c_str()),
                true
        };
    }
    if (!entry.PQGroupInfo) {
        return TProcessingResult {
                Ydb::PersQueue::ErrorCode::ErrorCode::ERROR,
                Sprintf("topic '%s' describe error, reason: could not retrieve topic description, Marker# PQ99",
                        topicName.c_str()),
                true
        };
    }
    auto& description = entry.PQGroupInfo->Description;
    if (!description.HasBalancerTabletID() || description.GetBalancerTabletID() == 0) {
        return TProcessingResult {
                Ydb::PersQueue::ErrorCode::ErrorCode::UNKNOWN_TOPIC,
                Sprintf("topic '%s' has no balancer, Marker# PQ193", topicName.c_str()),
                true
        };
    }
    return {};
}

} // namespace NKikimr::NGRpcProxy::V1
