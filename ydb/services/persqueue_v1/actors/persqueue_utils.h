#pragma once

#include <ydb/public/api/protos/persqueue_error_codes_v1.pb.h>

#include <ydb/library/aclib/aclib.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/public/api/protos/draft/persqueue_error_codes.pb.h> // double check

namespace NKikimr::NGRpcProxy::V1 {

#ifdef PQ_LOG_PREFIX
#undef PQ_LOG_PREFIX
#endif
#define PQ_LOG_PREFIX "session cookie " << Cookie << " consumer " << ClientPath << " session " << Session

static constexpr char KafkaPlainAuthPermission[] = "ydb.api.kafkaPlainAuth";
static constexpr char KafkaPlainAuthSid[] = "ydb.api.kafkaPlainAuth@as";

// moved to ydb/core/client/server/msgbus_server_persqueue.h?
// const TString& TopicPrefix(const TActorContext& ctx);

template<typename TItem0, typename... TItems>
bool AllEqual(const TItem0& item0, const TItems&... items) {
    return ((items == item0) && ... && true);
}

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

Ydb::StatusIds::StatusCode ConvertPersQueueInternalCodeToStatus(const Ydb::PersQueue::ErrorCode::ErrorCode code);
Ydb::StatusIds::StatusCode ConvertPersQueueInternalCodeToStatus(const NPersQueue::NErrorCode::EErrorCode code);

Ydb::PersQueue::ErrorCode::ErrorCode ConvertOldCode(const NPersQueue::NErrorCode::EErrorCode code);

// to suppress clangd false positive warning
// remove before ship
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-function"
static inline bool InternalErrorCode(Ydb::PersQueue::ErrorCode::ErrorCode errorCode) {
    switch(errorCode) {
        // TODO: check list
        case Ydb::PersQueue::ErrorCode::ERROR:
        case Ydb::PersQueue::ErrorCode::INITIALIZING:
        case Ydb::PersQueue::ErrorCode::OVERLOAD:
        case Ydb::PersQueue::ErrorCode::WRITE_ERROR_DISK_IS_FULL:
        case Ydb::PersQueue::ErrorCode::TABLET_PIPE_DISCONNECTED:
            return true;
        default:
            return false;
    }
    return false;
}
#pragma clang diagnostic pop

void FillIssue(Ydb::Issue::IssueMessage* issue, const Ydb::PersQueue::ErrorCode::ErrorCode errorCode, const TString& errorReason);


static inline TVector<TEvTicketParser::TEvAuthorizeTicket::TEntry>  GetTicketParserEntries(const TString& dbId, const TString& folderId, bool useKafkaApi = false) {
    TVector<TString> permissions = {
        "ydb.databases.list",
        "ydb.databases.create",
        "ydb.databases.connect",
        "ydb.tables.select",
        "ydb.schemas.getMetadata",
        "ydb.streams.write"
    };
    if (useKafkaApi) {
        permissions.push_back(KafkaPlainAuthPermission);
    }
    TVector<std::pair<TString, TString>> attributes;
    if (!dbId.empty()) attributes.push_back({"database_id", dbId});
    if (!folderId.empty()) attributes.push_back({"folder_id", folderId});
    if (!attributes.empty()) {
        return {{permissions, attributes}};
    }
    return {};
}

Ydb::PersQueue::ErrorCode::ErrorCode ConvertNavigateStatus(NSchemeCache::TSchemeCacheNavigate::EStatus status);

} //namespace NKikimr::NGRpcProxy::V1
