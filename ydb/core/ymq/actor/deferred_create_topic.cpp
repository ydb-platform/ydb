#include "deferred_create_topic.h"

#include "executor.h"
#include "log.h"

#include "action.h"
#include <ydb/core/ymq/actor/cfg/cfg.h>
#include <ydb/core/ymq/base/query_id.h>
#include <ydb/core/ymq/base/queue_path.h>
#include <ydb/core/ymq/queues/common/key_hashes.h>

#include <ydb/core/protos/schemeshard/operations.pb.h>

#include <ydb/public/lib/value/value.h>

#include <util/generic/guid.h>

namespace NKikimr::NSQS {

namespace {

using NKikimr::NClient::TValue;

enum class EPhase {
    ReadingAttrs,
    CreatingScheme,
    MarkingDb,
};

struct TLoadedAttrs {
    ui64 MessageRetentionMs = 0;
    ui64 DelayMs = 0;
    ui64 VisibilityMs = 0;
    ui64 ReceiveWaitMs = 0;
    bool HasContentBasedDeduplication = false;
    bool ContentBasedDeduplication = false;
    TString DlqName;
    ui64 MaxReceiveCount = 0;
};

static TString MakeMarkTopicCreatedProgram(const TString& root, bool isFifo) {
    const TString sub = isFifo ? ".FIFO" : ".STD";
    return TStringBuilder() << R"__(
(
    (let queueIdNumber      (Parameter 'QUEUE_ID_NUMBER (DataType 'Uint64)))
    (let queueIdNumberHash  (Parameter 'QUEUE_ID_NUMBER_HASH (DataType 'Uint64)))
    (let userName (Parameter 'USER_NAME (DataType 'Utf8)))
    (let name (Parameter 'NAME (DataType 'Utf8)))
    (let queuesTable ')__" << root << R"__(/.Queues)
    (let queuesRow '(
        '('Account userName)
        '('QueueName name)))
    (let queuesUpdate '(
        '('TopicCreated (Bool 'true))))
    (let attrsTable ')__" << root << "/" << sub << R"__(/Attributes)
    (let attrsRow '(
        '('QueueIdNumberHash queueIdNumberHash)
        '('QueueIdNumber queueIdNumber)))
    (let attrsUpdate '(
        '('TopicCreated (Bool 'true))))
    (return (AsList
        (UpdateRow queuesTable queuesRow queuesUpdate)
        (UpdateRow attrsTable attrsRow attrsUpdate)))
)
)__";
}

static THolder<TEvTxUserProxy::TEvProposeTransaction> BuildCreatePersQueueGroupTx(
    const TString& queuePath,
    const TString& versionName,
    const TString& accountName,
    const TString& folderId,
    bool isFifo,
    const TLoadedAttrs& attrs
) {
    auto ev = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
    auto* trans = ev->Record.MutableTransaction()->MutableModifyScheme();

    const TString topicDir = TString::Join(queuePath, '/', versionName);

    trans->SetWorkingDir(topicDir);
    trans->SetOperationType(NKikimrSchemeOp::ESchemeOpCreatePersQueueGroup);

    auto* pqgroup = trans->MutableCreatePersQueueGroup();
    pqgroup->SetName("streamImpl");
    pqgroup->SetTotalGroupCount(1);

    auto* config = pqgroup->MutablePQTabletConfig();
    config->SetTopicName("streamImpl");
    config->SetTopicPath(TString::Join(topicDir, '/', "streamImpl"));

    ui64 retentionMs = attrs.MessageRetentionMs;
    if (!retentionMs) {
        retentionMs = TDuration::Days(4).MilliSeconds();
    }
    const ui64 minRetentionMs = Cfg().GetMinMessageRetentionPeriodMs();
    if (retentionMs < minRetentionMs) {
        retentionMs = minRetentionMs;
    }
    config->MutablePartitionConfig()->SetLifetimeSeconds(Max<ui64>(1, retentionMs / 1000));

    if (attrs.HasContentBasedDeduplication) {
        config->SetContentBasedDeduplication(attrs.ContentBasedDeduplication);
    }

    auto* partitionStrategy = pqgroup->MutablePQTabletConfig()->MutablePartitionStrategy();
    partitionStrategy->SetPartitionStrategyType(::NKikimrPQ::TPQTabletConfig::CAN_SPLIT_AND_MERGE);
    partitionStrategy->SetMinPartitionCount(1);
    partitionStrategy->SetMaxPartitionCount(100);
    partitionStrategy->SetScaleUpPartitionWriteSpeedThresholdPercent(80);
    partitionStrategy->SetScaleDownPartitionWriteSpeedThresholdPercent(20);
    partitionStrategy->SetScaleThresholdSeconds(30);

    auto* consumer = config->AddConsumers();
    consumer->SetName(ConsumerName);
    consumer->SetType(::NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP);
    consumer->SetKeepMessageOrder(isFifo);
    if (attrs.DelayMs) {
        consumer->SetDefaultDelayMessageTimeMs(attrs.DelayMs);
    }
    if (attrs.VisibilityMs) {
        consumer->SetDefaultProcessingTimeoutSeconds(Max<ui64>(1, attrs.VisibilityMs / 1000));
    }
    if (attrs.ReceiveWaitMs) {
        consumer->SetDefaultReceiveMessageWaitTimeMs(attrs.ReceiveWaitMs);
    }
    if (attrs.MaxReceiveCount) {
        consumer->SetDeadLetterPolicyEnabled(true);
        consumer->SetDeadLetterPolicy(::NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_DELETE);
        consumer->SetMaxProcessingAttempts(attrs.MaxReceiveCount);
    }
    if (attrs.DlqName && attrs.MaxReceiveCount) {
        consumer->SetDeadLetterPolicyEnabled(true);
        consumer->SetDeadLetterPolicy(::NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE);
        consumer->SetDeadLetterQueue(TStringBuilder() << "sqs://" << accountName << "/" << folderId << "/" << attrs.DlqName);
    }

    return ev;
}

class TDeferredCreateTopicActor : public NActors::TActorBootstrapped<TDeferredCreateTopicActor> {
public:
    TDeferredCreateTopicActor(
        const NActors::TActorId& sqsServiceId,
        TString userName,
        TString queueName,
        TString folderId,
        bool isFifo,
        ui64 version,
        ui32 tablesFormat,
        TIntrusivePtr<TTransactionCounters> transactionCounters
    )
        : SqsServiceId_(sqsServiceId)
        , UserName_(std::move(userName))
        , QueueName_(std::move(queueName))
        , FolderId_(std::move(folderId))
        , IsFifo_(isFifo)
        , Version_(version)
        , TablesFormat_(tablesFormat)
        , TransactionCounters_(std::move(transactionCounters))
    {
    }

    void Bootstrap() {
        Become(&TDeferredCreateTopicActor::StateFunc);
        RequestReadAttributes();
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SQS_ACTOR;
    }

private:
    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TSqsEvents::TEvExecuted, HandleExecuted);
            cFunc(TEvPoisonPill::EventType, PassAway);
        default:
            break;
        }
    }

    void RequestReadAttributes() {
        Phase_ = EPhase::ReadingAttrs;
        const TString reqId = CreateGuidAsString();
        TExecutorBuilder(SelfId(), reqId)
            .User(UserName_)
            .Queue(QueueName_)
            .QueryId(INTERNAL_GET_QUEUE_ATTRIBUTES_ID)
            .QueueVersion(Version_)
            .Fifo(IsFifo_)
            .TablesFormat(TablesFormat_)
            .CreateExecutorActor(true)
            .RetryOnTimeout()
            .Counters(TransactionCounters_)
            .Params()
                .Uint64("QUEUE_ID_NUMBER", Version_)
                .Uint64("QUEUE_ID_NUMBER_HASH", GetKeysHash(Version_))
                .Utf8("NAME", QueueName_)
                .Utf8("USER_NAME", UserName_)
            .ParentBuilder()
            .Start();
    }

    void HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        switch (Phase_) {
        case EPhase::ReadingAttrs:
            OnReadAttributes(record);
            break;
        case EPhase::CreatingScheme:
            OnCreateScheme(record);
            break;
        case EPhase::MarkingDb:
            OnMarkDb(record);
            break;
        }
    }

    void OnReadAttributes(const NKikimrTxUserProxy::TEvProposeTransactionStatus& record) {
        if (record.GetStatus() != TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
            LOG_SQS_WARN("Deferred topic: read attributes failed for [" << UserName_ << "/" << QueueName_ << "]: " << record);
            Finish(false);
            return;
        }
        const TValue val(TValue::Create(record.GetExecutionEngineEvaluatedResponse()));
        if (!bool(val["queueExists"])) {
            LOG_SQS_WARN("Deferred topic: queue [" << UserName_ << "/" << QueueName_ << "] has no attributes row");
            Finish(false);
            return;
        }
        const TValue attrs(val["attrs"]);
        LoadedAttrs_.MessageRetentionMs = attrs["MessageRetentionPeriod"].HaveValue() ? ui64(attrs["MessageRetentionPeriod"]) : 0;
        LoadedAttrs_.DelayMs = attrs["DelaySeconds"].HaveValue() ? ui64(attrs["DelaySeconds"]) : 0;
        LoadedAttrs_.VisibilityMs = attrs["VisibilityTimeout"].HaveValue() ? ui64(attrs["VisibilityTimeout"]) : 0;
        LoadedAttrs_.ReceiveWaitMs = attrs["ReceiveMessageWaitTime"].HaveValue() ? ui64(attrs["ReceiveMessageWaitTime"]) : 0;
        if (attrs["ContentBasedDeduplication"].HaveValue()) {
            LoadedAttrs_.HasContentBasedDeduplication = true;
            LoadedAttrs_.ContentBasedDeduplication = bool(attrs["ContentBasedDeduplication"]);
        }
        const TValue dlqName = attrs["DlqName"];
        const TValue maxReceive = attrs["MaxReceiveCount"];
        if (dlqName.HaveValue()) {
            LoadedAttrs_.DlqName = TString(dlqName);
        }
        if (maxReceive.HaveValue()) {
            LoadedAttrs_.MaxReceiveCount = ui64(maxReceive);
        }

        RequestCreateTopicScheme();
    }

    void RequestCreateTopicScheme() {
        Phase_ = EPhase::CreatingScheme;
        const TQueuePath path(Cfg().GetRoot(), UserName_, QueueName_);
        const TString versionName = TStringBuilder() << "v" << Version_;
        auto tx = BuildCreatePersQueueGroupTx(path.GetQueuePath(), versionName, UserName_, FolderId_, IsFifo_, LoadedAttrs_);

        const TString reqId = CreateGuidAsString();
        const TQueuePath qpath(Cfg().GetRoot(), UserName_, QueueName_, Version_);
        Register(new TMiniKqlExecutionActor(
            SelfId(),
            reqId,
            std::move(tx),
            true,
            qpath,
            TransactionCounters_,
            TSqsEvents::TExecutedCallback()
        ));
    }

    void OnCreateScheme(const NKikimrTxUserProxy::TEvProposeTransactionStatus& record) {
        const auto status = record.GetStatus();
        if (status != TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete
            && status != TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecAlready)
        {
            LOG_SQS_WARN("Deferred topic: CreateTopic failed for [" << UserName_ << "/" << QueueName_ << "]: " << record);
            Finish(false);
            return;
        }
        RequestMarkTopicCreated();
    }

    void RequestMarkTopicCreated() {
        Phase_ = EPhase::MarkingDb;
        const TString reqId = CreateGuidAsString();
        const TString program = MakeMarkTopicCreatedProgram(Cfg().GetRoot(), IsFifo_);
        TExecutorBuilder(SelfId(), reqId)
            .User(UserName_)
            .Queue(QueueName_)
            .QueueVersion(Version_)
            .Fifo(IsFifo_)
            .TablesFormat(TablesFormat_)
            .CreateExecutorActor(true)
            .RetryOnTimeout()
            .Counters(TransactionCounters_)
            .Text(program)
            .Params()
                .Uint64("QUEUE_ID_NUMBER", Version_)
                .Uint64("QUEUE_ID_NUMBER_HASH", GetKeysHash(Version_))
                .Utf8("NAME", QueueName_)
                .Utf8("USER_NAME", UserName_)
            .ParentBuilder()
            .Start();
    }

    void OnMarkDb(const NKikimrTxUserProxy::TEvProposeTransactionStatus& record) {
        if (record.GetStatus() != TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
            LOG_SQS_WARN("Deferred topic: mark TopicCreated failed for [" << UserName_ << "/" << QueueName_ << "]: " << record);
            Finish(false);
            return;
        }
        LOG_SQS_INFO("Deferred topic: created PersQueue topic for [" << UserName_ << "/" << QueueName_ << "]");
        Finish(true);
    }

    void Finish(bool success) {
        auto done = MakeHolder<TSqsEvents::TEvDeferredTopicCreationResult>();
        done->UserName = UserName_;
        done->QueueName = QueueName_;
        done->Success = success;
        Send(SqsServiceId_, done.Release());
        PassAway();
    }

private:
    const NActors::TActorId SqsServiceId_;
    const TString UserName_;
    const TString QueueName_;
    const TString FolderId_;
    const bool IsFifo_;
    const ui64 Version_;
    const ui32 TablesFormat_;
    TIntrusivePtr<TTransactionCounters> TransactionCounters_;

    EPhase Phase_ = EPhase::ReadingAttrs;
    TLoadedAttrs LoadedAttrs_;
};

} // namespace

NActors::IActor* CreateDeferredCreateTopicActor(
    const NActors::TActorId& sqsServiceId,
    TString userName,
    TString queueName,
    TString folderId,
    bool isFifo,
    ui64 version,
    ui32 tablesFormat,
    TIntrusivePtr<TTransactionCounters> transactionCounters
) {
    return new TDeferredCreateTopicActor(
        sqsServiceId,
        std::move(userName),
        std::move(queueName),
        std::move(folderId),
        isFifo,
        version,
        tablesFormat,
        std::move(transactionCounters)
    );
}

} // namespace NKikimr::NSQS
