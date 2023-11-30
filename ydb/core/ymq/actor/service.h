#pragma once
#include "defs.h"
#include "events.h"
#include "log.h"
#include "serviceid.h"
#include "index_events_processor.h"

#include <ydb/core/ymq/base/events_writer_iface.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <util/generic/hash.h>
#include <util/generic/hash_multi_map.h>
#include <util/generic/ptr.h>
#include <library/cpp/logger/log.h>

namespace NKikimr::NSQS {

class TSqsService
    : public TActorBootstrapped<TSqsService>
{
public:
    void Bootstrap();

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SQS_SERVICE_ACTOR;
    }

    struct TUserInfo;
    using TUserInfoPtr = TIntrusivePtr<TUserInfo>;
    using TUsersMap = std::map<TString, TUserInfoPtr>;

private:
    struct TQueueInfo;

    using TQueueInfoPtr = TIntrusivePtr<TQueueInfo>;

    class TLocalLeaderManager;

    STATEFN(StateFunc);

    void InitSchemeCache();

    void HandleWakeup(TEvWakeup::TPtr& ev);
    void HandleDescribeSchemeResult(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev);
    void HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev);
    void HandlePipeClientConnected(TEvTabletPipe::TEvClientConnected::TPtr& ev);
    void HandlePipeClientDisconnected(TEvTabletPipe::TEvClientDestroyed::TPtr& ev);
    void HandleGetLeaderNodeForQueueRequest(TSqsEvents::TEvGetLeaderNodeForQueueRequest::TPtr& ev);
    void HandleGetConfiguration(TSqsEvents::TEvGetConfiguration::TPtr& ev);
    void HandleSqsRequest(TSqsEvents::TEvSqsRequest::TPtr& ev); // request from nodes with old version
    void HandleQueueLeaderDecRef(TSqsEvents::TEvQueueLeaderDecRef::TPtr& ev);
    void HandleGetQueueId(TSqsEvents::TEvGetQueueId::TPtr& ev);
    void HandleGetQueueFolderIdAndCustomName(TSqsEvents::TEvGetQueueFolderIdAndCustomName::TPtr& ev);
    void HandleCountQueues(TSqsEvents::TEvCountQueues::TPtr& ev);
    void HandleInsertQueueCounters(TSqsEvents::TEvInsertQueueCounters::TPtr& ev);
    void HandleUserSettingsChanged(TSqsEvents::TEvUserSettingsChanged::TPtr& ev);
    void HandleQueuesList(TSqsEvents::TEvQueuesList::TPtr& ev);
    void HandleReloadStateRequest(TSqsEvents::TEvReloadStateRequest::TPtr& ev);
    void HandleLeaderStarted(TSqsEvents::TEvLeaderStarted::TPtr& ev);
    void HandleNodeTrackingSubscriptionStatus(TSqsEvents::TEvNodeTrackerSubscriptionStatus::TPtr& ev);
    void CreateNodeTrackingSubscription(TQueueInfoPtr queueInfo);
    void CancleNodeTrackingSubscription(TQueueInfoPtr queueInfo);

    void ProcessConnectTimeoutToLeader();
    void ScheduleRequestSqsUsersList();
    void RequestSqsUsersList();

    void ScheduleRequestSqsQueuesList();
    void RequestSqsQueuesList();
    bool RequestQueueListForUser(const TUserInfoPtr& user, const TString& reqId) Y_WARN_UNUSED_RESULT;

    void RemoveQueue(const TString& userName, const TString& queue);
    TUsersMap::iterator MutableUserIter(const TString& userName, bool moveUserRequestsToUserRecord = true, bool* requestsWereMoved = nullptr);
    TUserInfoPtr MutableUser(const TString& userName, bool moveUserRequestsToUserRecord = true, bool* requestsWereMoved = nullptr);
    void RemoveUser(const TString& userName);
    std::map<TString, TQueueInfoPtr>::iterator AddQueue(const TString& userName, const TString& queue, ui64 leaderTabletId,
                                                        const TString& customName, const TString& folderId, const ui32 tablesFormat, const ui64 version,
                                                        const ui64 shardsCount, const TInstant createdTimestamp, bool isFifo);

    void AnswerNoUserToRequests();
    void AnswerNoQueueToRequests(const TUserInfoPtr& user);
    void AnswerThrottledToRequests(const TUserInfoPtr& user);

    void AnswerErrorToRequests();
    void AnswerErrorToRequests(const TUserInfoPtr& user);

    void AnswerLeaderlessConfiguration(TSqsEvents::TEvGetConfiguration::TPtr& ev, const TUserInfoPtr& userInfo, const TQueueInfoPtr& queueInfo);
    void ProcessConfigurationRequestForQueue(TSqsEvents::TEvGetConfiguration::TPtr& ev, const TUserInfoPtr& userInfo, const TQueueInfoPtr& queueInfo);

    void IncLocalLeaderRef(const TActorId& referer, const TQueueInfoPtr& queueInfo, const TString& reason);
    void DecLocalLeaderRef(const TActorId& referer, const TString& reason);

    template <class TEvent>
    TUserInfoPtr GetUserOrWait(TAutoPtr<TEvent>& ev);

    void InsertWaitingRequest(TSqsEvents::TEvGetQueueId::TPtr&& ev);
    void InsertWaitingRequest(TSqsEvents::TEvGetQueueFolderIdAndCustomName::TPtr&& ev);
    void InsertWaitingRequest(TSqsEvents::TEvGetConfiguration::TPtr&& ev);
    void InsertWaitingRequest(TSqsEvents::TEvGetLeaderNodeForQueueRequest::TPtr&& ev);
    void InsertWaitingRequest(TSqsEvents::TEvCountQueues::TPtr&& ev);

    void InsertWaitingRequest(TSqsEvents::TEvGetQueueId::TPtr&& ev, const TUserInfoPtr& userInfo);
    void InsertWaitingRequest(TSqsEvents::TEvGetQueueFolderIdAndCustomName::TPtr&& ev, const TUserInfoPtr& userInfo);
    void InsertWaitingRequest(TSqsEvents::TEvGetConfiguration::TPtr&& ev, const TUserInfoPtr& userInfo);
    void InsertWaitingRequest(TSqsEvents::TEvGetLeaderNodeForQueueRequest::TPtr&& ev, const TUserInfoPtr& userInfo);
    void InsertWaitingRequest(TSqsEvents::TEvCountQueues::TPtr&& ev, const TUserInfoPtr& userInfo);

    template <class TMultimap>
    size_t MoveUserRequests(const TUserInfoPtr& userInfo, TMultimap& map); // returns moved requests count
    template <class TMultimap>
    void AnswerNoUserToRequests(TMultimap& map);
    template <class TMultimap>
    void AnswerNoQueueToRequests(const TUserInfoPtr& user, TMultimap& map);
    template <class TMultimap>
    void AnswerErrorToRequests(const TUserInfoPtr& user, TMultimap& map);
    template <class TMultimap>
    void AnswerThrottledToRequests(TMultimap& map);

    void AnswerNotExists(TSqsEvents::TEvGetLeaderNodeForQueueRequest::TPtr& ev, const TUserInfoPtr& userInfo);
    void AnswerNotExists(TSqsEvents::TEvGetConfiguration::TPtr& ev, const TUserInfoPtr& userInfo);
    void AnswerNotExists(TSqsEvents::TEvGetQueueId::TPtr& ev, const TUserInfoPtr& userInfo);
    void AnswerNotExists(TSqsEvents::TEvGetQueueFolderIdAndCustomName::TPtr& ev, const TUserInfoPtr& userInfo);
    void AnswerNotExists(TSqsEvents::TEvCountQueues::TPtr& ev, const TUserInfoPtr& userInfo);

    void AnswerFailed(TSqsEvents::TEvGetLeaderNodeForQueueRequest::TPtr& ev, const TUserInfoPtr& userInfo);
    void AnswerFailed(TSqsEvents::TEvGetConfiguration::TPtr& ev, const TUserInfoPtr& userInfo);
    void AnswerFailed(TSqsEvents::TEvGetQueueId::TPtr& ev, const TUserInfoPtr& userInfo);
    void AnswerFailed(TSqsEvents::TEvGetQueueFolderIdAndCustomName::TPtr& ev, const TUserInfoPtr& userInfo);
    void AnswerFailed(TSqsEvents::TEvCountQueues::TPtr& ev, const TUserInfoPtr& userInfo);

    void AnswerThrottled(TSqsEvents::TEvGetLeaderNodeForQueueRequest::TPtr& ev);
    void AnswerThrottled(TSqsEvents::TEvGetConfiguration::TPtr& ev);
    void AnswerThrottled(TSqsEvents::TEvGetQueueId::TPtr& ev);
    void AnswerThrottled(TSqsEvents::TEvGetQueueFolderIdAndCustomName::TPtr& ev);

    void Answer(TSqsEvents::TEvGetQueueFolderIdAndCustomName::TPtr& ev, const TQueueInfoPtr& queueInfo);

    void AnswerCountQueuesRequests(const TUserInfoPtr& user);

    void NotifyLocalDeadLetterQueuesLeaders(const std::vector<TSqsEvents::TEvQueuesList::TQueueRecord>& sortedQueues) const;

    void MakeAndRegisterYcEventsProcessor();

private:
    TString RootUrl_;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> SqsCoreCounters_;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> YmqRootCounters_;
    std::shared_ptr<TAlignedPagePoolCounters> AllocPoolCounters_;
    TIntrusivePtr<TUserCounters> AggregatedUserCounters_;
    TIntrusivePtr<TMonitoringCounters> MonitoringCounters_;
    TUsersMap Users_;
    THashMap<TActorId, TQueueInfoPtr> LocalLeaderRefs_; // referer -> queue info
    TActorId SchemeCache_;
    TActorId QueuesListReader_;

    // State machine
    bool RequestingUsersList_ = false;
    bool ScheduledRequestingUsersList_ = false;
    size_t EarlyRequestUsersListBudget_ = 0; // Defence from continuously requesting users list.
    TInstant LastRequestUsersListTime_;

    bool RequestingQueuesList_ = false;
    bool ScheduledRequestingQueuesList_ = false;
    i64 EarlyRequestQueuesListMinBudget_ = 0; // Defence from continuously requesting queues list.
    TInstant LastRequestQueuesListTime_;
    THashMultiMap<TString, TSqsEvents::TEvGetLeaderNodeForQueueRequest::TPtr> GetLeaderNodeRequests_; // user name -> request
    THashMultiMap<TString, TSqsEvents::TEvGetConfiguration::TPtr> GetConfigurationRequests_; // user name -> request
    THashMultiMap<TString, TSqsEvents::TEvGetQueueId::TPtr> GetQueueIdRequests_; // user name -> request
    THashMultiMap<TString, TSqsEvents::TEvGetQueueFolderIdAndCustomName::TPtr> GetQueueFolderIdAndCustomNameRequests_; // user name -> request
    THashMultiMap<TString, TSqsEvents::TEvCountQueues::TPtr> CountQueuesRequests_; // user name -> request

    TActorId NodeTrackerActor_;
    THashMap<ui64, TQueueInfoPtr> QueuePerNodeTrackingSubscription;
    ui64 MaxNodeTrackingSubscriptionId = 0;

    THashSet<TQueueInfoPtr> QueuesWithGetNodeWaitingRequests;

    struct TYcSearchEventsConfig {
        TString Database;
        bool Enabled = false;
        bool TenantMode = false;
        TDuration ReindexInterval = TDuration::Hours(4);
        TDuration RescanInterval = TDuration::Minutes(1);
    };
    TYcSearchEventsConfig YcSearchEventsConfig;
    THolder<TLocalLeaderManager> LocalLeaderManager;
};

} // namespace NKikimr::NSQS
