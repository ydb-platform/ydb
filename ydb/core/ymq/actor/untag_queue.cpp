#include "action.h"
#include "error.h"
#include "executor.h"
#include "log.h"

#include <ydb/core/ymq/actor/cloud_events/cloud_events.h>

#include <ydb/core/ymq/base/constants.h>
#include <ydb/core/ymq/base/helpers.h>
#include <ydb/core/ymq/base/limits.h>
#include <ydb/core/ymq/base/dlq_helpers.h>
#include <ydb/core/ymq/queues/common/key_hashes.h>
#include <ydb/public/lib/value/value.h>

#include <util/string/cast.h>

using NKikimr::NClient::TValue;

namespace NKikimr::NSQS {

class TUntagQueueActor
    : public TActionActor<TUntagQueueActor>
{
public:
    static constexpr bool NeedQueueAttributes() {
        return true;
    }
    static constexpr bool NeedQueueTags() {
        return true;
    }

    TUntagQueueActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb)
        : TActionActor(sourceSqsRequest, EAction::UntagQueue, std::move(cb))
    {
        for (const auto& key : Request().tagkeys()) {
            TagKeys_.push_back(key);
        }

        IsCloudEventsEnabled = Cfg().HasCloudEventsConfig() && Cfg().GetCloudEventsConfig().GetEnableCloudEvents();
    }

private:
    bool DoValidate() override {
        if (!GetQueueName()) {
            MakeError(Response_.MutableUntagQueue(), NErrors::MISSING_PARAMETER, "No QueueName parameter.");
            return false;
        }

        return true;
    }

    TError* MutableErrorDesc() override {
        return Response_.MutableUntagQueue()->MutableError();
    }

    void DoAction() override {
        Become(&TThis::StateFunc);

        auto oldTags = TagsToJson(*QueueTags_);

        NJson::TJsonMap tags;
        if (QueueTags_.Defined()) {
            for (const auto& k : TagKeys_) {
                QueueTags_->GetMapSafe().erase(k);
            }
            for (const auto& [k, v] : QueueTags_->GetMap()) {
                tags[k] = v;
            }
        }

        TExecutorBuilder builder(SelfId(), RequestId_);
        builder
            .User(UserName_)
            .Queue(GetQueueName())
            .TablesFormat(TablesFormat())
            .QueueLeader(QueueLeader_)
            .QueryId(TAG_QUEUE_ID)
            .Counters(QueueCounters_)
            .RetryOnTimeout()
            .Params()
                .Utf8("NAME", GetQueueName())
                .Utf8("USER_NAME", UserName_)
                .Utf8("TAGS", TagsToJson(tags))
                .Utf8("OLD_TAGS", oldTags);

        builder.Start();
    }

    TString DoGetQueueName() const override {
        return Request().GetQueueName();
    }

    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWakeup, HandleWakeup);
            hFunc(TSqsEvents::TEvExecuted, HandleExecuted);
        }
    }

    TString GetFullCloudEventsTablePath() const {
        if (!Cfg().GetRoot().empty()) {
            return TStringBuilder() << Cfg().GetRoot() << "/" << NCloudEvents::TProcessor::EventTableName;
        } else {
            return TString(NCloudEvents::TProcessor::EventTableName);
        }
    }

    void AddCloudEventLog() const {
        Y_ABORT_UNLESS(IsCloudEventsEnabled);
        const auto& cloudEvCfg = Cfg().GetCloudEventsConfig();
        TString database = (cloudEvCfg.HasTenantMode() && cloudEvCfg.GetTenantMode()? Cfg().GetRoot() : "");

        auto evId = NCloudEvents::TEventIdGenerator::Generate();
        auto createdAt = TInstant::Now().MilliSeconds();

        TString query = TStringBuilder()
            << "UPSERT INTO `" << GetFullCloudEventsTablePath() << "`\n"
            << "("
                << "CreatedAt,"
                << "Id,"
                << "QueueName,"
                << "Type,"
                << "CloudId,"
                << "FolderId,"
                << "UserSID,"
                << "UserSanitizedToken,"
                << "AuthType,"
                << "PeerName,"
                << "RequestId,"
                << "IdempotencyId,"
                << "Labels"
            << ")" << "\n"
            << "VALUES" << "\n"
            << "("
                << createdAt << ","
                << evId << ","
                << "\"" << GetQueueName() << "\"" << ","
                << "\"" << "UpdateMessageQueue" << "\"" << ","
                << "\"" << UserName_ << "\"" << ","
                << "\"" << FolderId_ << "\"" << ","
                << "\"" << UserSID_ << "\"" << ","
                << "\"" << SanitizedToken_ << "\"" << ","
                << "\"" << "DEFAULT_AuthType" << "\"" << ","
                << "\"" << "DEFAULT_PeerName" << "\"" << ","
                << "\"" << RequestId_ << "\"" << ","
                << "\"" << "DEFAULT_IdempotencyId" << "\"" << ","
                << "\"" << TagsToJson(*QueueTags_) << "\""
            << ");" << "\n";

        auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        auto* request = ev->Record.MutableRequest();

        if (!database.empty()) {
            request->SetDatabase(database);
        }

        request->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        request->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
        request->SetQuery(query);

        request->MutableQueryCachePolicy()->set_keep_in_cache(true);
        request->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
        request->MutableTxControl()->set_commit_tx(true);

        Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), ev.Release());
    }

    void HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const ui32 status = record.GetStatus();
        auto* result = Response_.MutableUntagQueue();

        if (status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
            const TValue val(TValue::Create(record.GetExecutionEngineEvaluatedResponse()));
            bool updated = val["updated"];
            if (updated) {
                if (IsCloudEventsEnabled) {
                    AddCloudEventLog();
                }

                RLOG_SQS_DEBUG("Sending clear attributes cache event for queue [" << UserName_ << "/" << GetQueueName() << "]");
                Send(QueueLeader_, MakeHolder<TSqsEvents::TEvClearQueueAttributesCache>());
            } else {
                auto message = "Untag queue query failed, conflicting query in parallel";
                RLOG_SQS_ERROR(message << ": " << record);
                MakeError(result, NErrors::INTERNAL_FAILURE, message);
            }
        } else {
            RLOG_SQS_ERROR("Tag queue query failed, answer: " << record);
            MakeError(result, NErrors::INTERNAL_FAILURE);
        }
        SendReplyAndDie();
    }

    const TUntagQueueRequest& Request() const {
        return SourceSqsRequest_.GetUntagQueue();
    }

private:
    TVector<TString> TagKeys_;
    bool IsCloudEventsEnabled;
};

IActor* CreateUntagQueueActor(const NKikimrClient::TSqsRequest& sourceSqsRequest, THolder<IReplyCallback> cb) {
    return new TUntagQueueActor(sourceSqsRequest, std::move(cb));
}

} // namespace NKikimr::NSQS
