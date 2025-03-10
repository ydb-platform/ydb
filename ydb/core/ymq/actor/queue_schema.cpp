#include "cfg.h"
#include "executor.h"
#include "log.h"
#include "params.h"
#include "queue_schema.h"
#include "serviceid.h"

#include <ydb/core/ymq/base/limits.h>
#include <ydb/core/ymq/queues/common/db_queries_maker.h>
#include <ydb/core/ymq/queues/common/key_hashes.h>
#include <ydb/core/ymq/queues/fifo/schema.h>
#include <ydb/core/ymq/queues/std/schema.h>

#include <util/generic/utility.h>
#include <util/generic/guid.h>


using NKikimr::NClient::TValue;

namespace NKikimr::NSQS {

constexpr TStringBuf FIFO_TABLES_DIR = ".FIFO";
constexpr TStringBuf STD_TABLES_DIR = ".STD";

static bool IsGoodStatusCode(ui32 code) {
    switch (NTxProxy::TResultStatus::EStatus(code)) {
        case NTxProxy::TResultStatus::EStatus::ExecComplete:
        case NTxProxy::TResultStatus::EStatus::ExecAlready:
            return true;
        default:
            return false;
    }
}

TCreateQueueSchemaActorV2::TCreateQueueSchemaActorV2(const TQueuePath& path,
                                                     const TCreateQueueRequest& req,
                                                     const TActorId& sender,
                                                     const TString& requestId,
                                                     const TString& customQueueName,
                                                     const TString& folderId,
                                                     const bool isCloudMode,
                                                     const bool enableQueueAttributesValidation,
                                                     TIntrusivePtr<TUserCounters> userCounters,
                                                     TIntrusivePtr<TSqsEvents::TQuoterResourcesForActions> quoterResources,
                                                     const TString& tagsJson)
    : QueuePath_(path)
    , Request_(req)
    , Sender_(sender)
    , CustomQueueName_(customQueueName)
    , FolderId_(folderId)
    , RequestId_(requestId)
    , GeneratedQueueId_(CreateGuidAsString())
    , QueueCreationTimestamp_(TInstant::Now())
    , IsCloudMode_(isCloudMode)
    , EnableQueueAttributesValidation_(enableQueueAttributesValidation)
    , UserCounters_(std::move(userCounters))
    , QuoterResources_(std::move(quoterResources))
    , TagsJson_(tagsJson)
{
    IsFifo_ = AsciiHasSuffixIgnoreCase(IsCloudMode_ ? CustomQueueName_ : QueuePath_.QueueName, ".fifo");

    if (IsFifo_) {
        RequiredShardsCount_ = 1;
        RequiredTables_ = GetFifoTables();
    } else {
        RequiredShardsCount_ = Request_.GetShards();
        RequiredTables_ = GetStandardTables(RequiredShardsCount_, req.GetPartitions(), req.GetEnableAutosplit(), req.GetSizeToSplit());
    }
}

TCreateQueueSchemaActorV2::~TCreateQueueSchemaActorV2() = default;

static THolder<TSqsEvents::TEvQueueCreated> MakeErrorResponse(const TErrorClass& errorClass) {
    auto resp = MakeHolder<TSqsEvents::TEvQueueCreated>();
    resp->Success = false;
    resp->State = EQueueState::Active;
    resp->ErrorClass = &errorClass;

    return resp;
}

template<typename TMaybeAttribute, typename TValueType>
static void SetDefaultIfMissing(TMaybeAttribute& attribute, const TValueType& defaultVal) {
    if (!attribute) {
        attribute = defaultVal;
    }
}

static ui64 SecondsToMs(ui64 seconds) {
    return TDuration::Seconds(seconds).MilliSeconds();
}

void TCreateQueueSchemaActorV2::InitMissingQueueAttributes(const NKikimrConfig::TSqsConfig& config) {
    // https://docs.aws.amazon.com/en_us/AWSSimpleQueueService/latest/APIReference/API_CreateQueue.html

    SetDefaultIfMissing(ValidatedAttributes_.DelaySeconds, 0);
    SetDefaultIfMissing(ValidatedAttributes_.MaximumMessageSize, 256 * 1024); // 256 KB
    SetDefaultIfMissing(ValidatedAttributes_.MessageRetentionPeriod,
                        Max(TDuration::MilliSeconds(config.GetMinMessageRetentionPeriodMs()).Seconds(), TDuration::Days(4).Seconds()));
    SetDefaultIfMissing(ValidatedAttributes_.ReceiveMessageWaitTimeSeconds, 0); // seconds
    SetDefaultIfMissing(ValidatedAttributes_.VisibilityTimeout, 30); // seconds
    SetDefaultIfMissing(ValidatedAttributes_.ContentBasedDeduplication, false); // bool

    // RedrivePolicy could be unspecified
}

void TCreateQueueSchemaActorV2::Bootstrap() {
    Become(&TCreateQueueSchemaActorV2::Preamble);

    THashMap<TString, TString> attributes;
    for (const auto& attr : Request_.attributes()) {
        attributes[attr.GetName()] = attr.GetValue();
    }

    const bool clampValues = !EnableQueueAttributesValidation_;
    ValidatedAttributes_ = TQueueAttributes::FromAttributesAndConfig(attributes, Cfg(), IsFifo_, clampValues);

    if (!ValidatedAttributes_.Validate()) {
        auto resp = MakeErrorResponse(NErrors::VALIDATION_ERROR);
        resp->Error = ValidatedAttributes_.ErrorText;

        Send(Sender_, std::move(resp));
        PassAway();
        return;
    }

    if (ValidatedAttributes_.HasClampedAttributes()) {
        RLOG_SQS_WARN("Clamped some queue attribute values for account " << QueuePath_.UserName << " and queue name " << QueuePath_.QueueName);
    }

    InitMissingQueueAttributes(Cfg());

    if (ValidatedAttributes_.RedrivePolicy.TargetQueueName) {
        const TString createdQueueName = IsCloudMode_ ? CustomQueueName_ : QueuePath_.QueueName;
        auto resp = MakeErrorResponse(NErrors::VALIDATION_ERROR);
        if (ValidatedAttributes_.RedrivePolicy.TargetQueueName->empty()) {
            resp->Error = "Empty target dead letter queue name.";
        } else if (*ValidatedAttributes_.RedrivePolicy.TargetQueueName == createdQueueName) {
            resp->Error = "Using the queue itself as a dead letter queue is not allowed.";
        } else {
            Send(MakeSqsServiceID(SelfId().NodeId()),
                                      new TSqsEvents::TEvGetQueueId(RequestId_, QueuePath_.UserName,
                                      *ValidatedAttributes_.RedrivePolicy.TargetQueueName, FolderId_));
            return;
        }

        Send(Sender_, std::move(resp));
        PassAway();

        return;
    }

    RequestQueueParams();
}

static const char* const ReadQueueParamsQueryCloud = R"__(
    (
        (let customName (Parameter 'CUSTOMNAME (DataType 'Utf8String)))
        (let folderId   (Parameter 'FOLDERID   (DataType 'Utf8String)))
        (let defaultMaxQueuesCount (Parameter 'DEFAULT_MAX_QUEUES_COUNT (DataType 'Uint64)))
        (let userName   (Parameter 'USER_NAME  (DataType 'Utf8String)))

        (let queuesTable '%1$s/.Queues)
        (let settingsTable '%1$s/.Settings)

        (let maxQueuesCountSettingRow '(
            '('Account userName)
            '('Name (Utf8String '"MaxQueuesCount"))))
        (let maxQueuesCountSettingSelect '(
            'Value))
        (let maxQueuesCountSettingRead (SelectRow settingsTable maxQueuesCountSettingRow maxQueuesCountSettingSelect))
        (let maxQueuesCountSetting (Coalesce (If (Exists maxQueuesCountSettingRead) (Cast (Member maxQueuesCountSettingRead 'Value) 'Uint64) defaultMaxQueuesCount) (Uint64 '0)))

        (let queuesRange '(
            '('Account userName userName)
            '('QueueName (Utf8String '"") (Void))))
        (let queues
            (Member (SelectRange queuesTable queuesRange '('QueueName 'CustomQueueName 'Version 'FolderId 'TablesFormat) '()) 'List))
        (let overLimit
            (LessOrEqual maxQueuesCountSetting (Length queues)))

        (let existingQueuesWithSameNameAndFolderId
            (Filter queues (lambda '(item) (block '(
                (return (Coalesce
                    (And
                        (Equal (Member item 'CustomQueueName) customName)
                        (Equal (Member item 'FolderId) folderId))
                    (Bool 'false))))))
            )
        )

        (let queueExists (NotEqual (Uint64 '0) (Length existingQueuesWithSameNameAndFolderId)))
        (let currentVersion (Coalesce
            (Member (ToOptional existingQueuesWithSameNameAndFolderId) 'Version)
            (Uint64 '0)
        ))
        (let currentTablesFormat (Coalesce
            (Member (ToOptional existingQueuesWithSameNameAndFolderId) 'TablesFormat)
            (Uint32 '0)
        ))
        (let existingResourceId (Coalesce
            (Member (ToOptional existingQueuesWithSameNameAndFolderId) 'QueueName)
            (Utf8String '"")
        ))

        (return (AsList
            (SetResult 'exists queueExists)
            (SetResult 'resourceId existingResourceId)
            (SetResult 'overLimit overLimit)
            (SetResult 'version currentVersion)
            (SetResult 'tablesFormat currentTablesFormat)))
    )
)__";

static const char* const ReadQueueParamsQueryYandex = R"__(
    (
        (let name       (Parameter 'NAME       (DataType 'Utf8String)))
        (let defaultMaxQueuesCount (Parameter 'DEFAULT_MAX_QUEUES_COUNT (DataType 'Uint64)))
        (let userName   (Parameter 'USER_NAME (DataType 'Utf8String)))

        (let queuesTable '%1$s/.Queues)
        (let settingsTable '%1$s/.Settings)

        (let maxQueuesCountSettingRow '(
            '('Account userName)
            '('Name (Utf8String '"MaxQueuesCount"))))
        (let maxQueuesCountSettingSelect '(
            'Value))
        (let maxQueuesCountSettingRead (SelectRow settingsTable maxQueuesCountSettingRow maxQueuesCountSettingSelect))
        (let maxQueuesCountSetting (Coalesce (If (Exists maxQueuesCountSettingRead) (Cast (Member maxQueuesCountSettingRead 'Value) 'Uint64) defaultMaxQueuesCount) (Uint64 '0)))

        (let queuesRange '(
            '('Account userName userName)
            '('QueueName (Utf8String '"") (Void))))
        (let queues
            (Member (SelectRange queuesTable queuesRange '('QueueState) '()) 'List))
        (let overLimit
            (LessOrEqual maxQueuesCountSetting (Length queues)))

        (let queuesRow '(
            '('Account userName)
            '('QueueName name)))
        (let queuesSelect '(
            'QueueState
            'Version
            'TablesFormat))
        (let queuesRead (SelectRow queuesTable queuesRow queuesSelect))

        (let queueExists
            (Coalesce
                (Or
                    (Equal (Uint64 '1) (Member queuesRead 'QueueState))
                    (Equal (Uint64 '3) (Member queuesRead 'QueueState))
                )
                (Bool 'false)))

        (let currentVersion
            (Coalesce
                (Member queuesRead 'Version)
                (Uint64 '0)
            )
        )
        (let currentTablesFormat
            (Coalesce
                (Member queuesRead 'TablesFormat)
                (Uint32 '0)
            )
        )

        (return (AsList
            (SetResult 'exists queueExists)
            (SetResult 'overLimit overLimit)
            (SetResult 'version currentVersion)
            (SetResult 'tablesFormat currentTablesFormat)))
    )
)__";

void TCreateQueueSchemaActorV2::RequestQueueParams() {
    if (IsCloudMode_) {
        auto ev = MakeExecuteEvent(Sprintf(ReadQueueParamsQueryCloud, Cfg().GetRoot().c_str()));
        auto* trans = ev->Record.MutableTransaction()->MutableMiniKQLTransaction();
        TParameters(trans->MutableParams()->MutableProto())
            .Utf8("CUSTOMNAME", CustomQueueName_)
            .Utf8("FOLDERID", FolderId_)
            .Uint64("DEFAULT_MAX_QUEUES_COUNT", Cfg().GetAccountSettingsDefaults().GetMaxQueuesCount())
            .Utf8("USER_NAME", QueuePath_.UserName);

        Register(new TMiniKqlExecutionActor(SelfId(), RequestId_, std::move(ev), false, QueuePath_, GetTransactionCounters(UserCounters_)));
    } else {
        auto ev = MakeExecuteEvent(Sprintf(ReadQueueParamsQueryYandex, Cfg().GetRoot().c_str()));
        auto* trans = ev->Record.MutableTransaction()->MutableMiniKQLTransaction();
        TParameters(trans->MutableParams()->MutableProto())
            .Utf8("NAME", QueuePath_.QueueName)
            .Uint64("DEFAULT_MAX_QUEUES_COUNT", Cfg().GetAccountSettingsDefaults().GetMaxQueuesCount())
            .Utf8("USER_NAME", QueuePath_.UserName);

        Register(new TMiniKqlExecutionActor(SelfId(), RequestId_, std::move(ev), false, QueuePath_, GetTransactionCounters(UserCounters_)));
    }
}

STATEFN(TCreateQueueSchemaActorV2::Preamble) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TSqsEvents::TEvQueueId,  HandleQueueId);
        hFunc(TSqsEvents::TEvExecuted, OnReadQueueParams);
        hFunc(TEvQuota::TEvClearance, OnCreateQueueQuota);
        hFunc(TSqsEvents::TEvAtomicCounterIncrementResult, OnAtomicCounterIncrement);
        cFunc(TEvPoisonPill::EventType, PassAway);
    }
}

void TCreateQueueSchemaActorV2::HandleQueueId(TSqsEvents::TEvQueueId::TPtr& ev) {
    THolder<TSqsEvents::TEvQueueCreated> resp;
    if (ev->Get()->Failed) {
        RLOG_SQS_WARN("Get queue id failed");
        resp = MakeErrorResponse(NErrors::INTERNAL_FAILURE);
    } else if (!ev->Get()->Exists) {
        resp = MakeErrorResponse(NErrors::VALIDATION_ERROR);
        resp->Error = "Target DLQ does not exist";
    } else {
        RequestQueueParams();
        return;
    }

    Y_ABORT_UNLESS(resp);
    Send(Sender_, std::move(resp));
    PassAway();
}

void TCreateQueueSchemaActorV2::OnReadQueueParams(TSqsEvents::TEvExecuted::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    const auto status = record.GetStatus();

    THolder<TSqsEvents::TEvQueueCreated> resp;

    if (status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
        const TValue val(TValue::Create(record.GetExecutionEngineEvaluatedResponse()));
        if (bool(val["exists"])) {
            if (IsCloudMode_) {
                ExistingQueueResourceId_ = TString(val["resourceId"]);
            }
            const ui64 currentVersion = ui64(val["version"]);
            const ui32 currentTablesFormat = ui32(val["tablesFormat"]);
            MatchQueueAttributes(currentVersion, currentTablesFormat);
            return;
        } else {
            if (bool(val["overLimit"])) {
                resp = MakeErrorResponse(NErrors::OVER_LIMIT);
                resp->Error = "Too many queues.";
            } else {
                if (Cfg().GetQuotingConfig().GetEnableQuoting() && QuoterResources_) {
                    RequestCreateQueueQuota();
                } else {
                    RunAtomicCounterIncrement();
                }
                return;
            }
        }
    } else {
        resp = MakeErrorResponse(NErrors::INTERNAL_FAILURE);
        resp->Error = "Failed to read queue params.";
        RLOG_SQS_ERROR("Failed to read queue params: " << record);
    }

    Y_ABORT_UNLESS(resp);
    Send(Sender_, std::move(resp));
    PassAway();
}

void TCreateQueueSchemaActorV2::RequestCreateQueueQuota() {
    TDuration deadline = TDuration::Max();
    const auto& quotingConfig = Cfg().GetQuotingConfig();
    if (quotingConfig.HasQuotaDeadlineMs()) {
        deadline = TDuration::MilliSeconds(quotingConfig.GetQuotaDeadlineMs());
    }
    Send(MakeQuoterServiceID(),
        new TEvQuota::TEvRequest(
            TEvQuota::EResourceOperator::And,
            { TEvQuota::TResourceLeaf(QuoterResources_->CreateQueueAction.QuoterId, QuoterResources_->CreateQueueAction.ResourceId, 1) },
            deadline));
}

void TCreateQueueSchemaActorV2::OnCreateQueueQuota(TEvQuota::TEvClearance::TPtr& ev) {
        switch (ev->Get()->Result) {
        case TEvQuota::TEvClearance::EResult::GenericError:
        case TEvQuota::TEvClearance::EResult::UnknownResource: {
            RLOG_SQS_ERROR("Failed to get quota for queue creation: " << ev->Get()->Result);
            Send(Sender_, MakeErrorResponse(NErrors::INTERNAL_FAILURE));
            PassAway();
            break;
        }
        case TEvQuota::TEvClearance::EResult::Deadline: {
            RLOG_SQS_WARN("Failed to get quota for queue creation: deadline expired");
            Send(Sender_, MakeErrorResponse(NErrors::THROTTLING_EXCEPTION));
            PassAway();
            break;
        }
        case TEvQuota::TEvClearance::EResult::Success: {
            RLOG_SQS_DEBUG("Successfully got quota for create queue request");
            RunAtomicCounterIncrement();
            break;
        }
        }
}

void TCreateQueueSchemaActorV2::RunAtomicCounterIncrement() {
    Register(new TAtomicCounterActor(SelfId(), QueuePath_.GetRootPath(), RequestId_));
}

void TCreateQueueSchemaActorV2::OnAtomicCounterIncrement(TSqsEvents::TEvAtomicCounterIncrementResult::TPtr& ev) {
    auto event = ev->Get();
    if (event->Success) {
        Become(&TCreateQueueSchemaActorV2::CreateComponentsState);
        Version_ = event->NewValue;
        VersionName_ = "v" + ToString(Version_); // add "v" prefix to provide the difference with deprecated version shards
        VersionedQueueFullPath_ = TString::Join(QueuePath_.GetQueuePath(), '/', VersionName_);
        CreateComponents();
        return;
    } else {
        auto resp = MakeErrorResponse(NErrors::INTERNAL_FAILURE);
        resp->Error = "Failed to create unique id.";
        resp->State = EQueueState::Creating;
        Send(Sender_, std::move(resp));
    }

    PassAway();
}

static const char* const GetTablesFormatQuery = R"__(
    (
        (let defaultTablesFormat (Parameter 'DEFAULT_TABLES_FORMAT (DataType 'Uint32)))
        (let userName   (Parameter 'USER_NAME  (DataType 'Utf8String)))
        (let settingsTable '%1$s/.Settings)
        (let tablesFormatSettingRow '(
            '('Account userName)
            '('Name (Utf8String '"CreateQueuesWithTabletFormat"))))
        (let tablesFormatSettingSelect '('Value))
        (let tablesFormatSettingRead (SelectRow settingsTable tablesFormatSettingRow tablesFormatSettingSelect))
        (let tablesFormatSetting
            (If (Exists tablesFormatSettingRead)
                (Cast (Member tablesFormatSettingRead 'Value) 'Uint32)
                defaultTablesFormat
            )
        )

        (return (AsList
            (SetResult 'tablesFormat tablesFormatSetting)
                )
        )
    )
)__";

void TCreateQueueSchemaActorV2::RequestTablesFormatSettings(const TString& accountName) {
    auto ev = MakeExecuteEvent(Sprintf(GetTablesFormatQuery, Cfg().GetRoot().c_str()));
    auto* trans = ev->Record.MutableTransaction()->MutableMiniKQLTransaction();
    TParameters(trans->MutableParams()->MutableProto())
        .Utf8("USER_NAME", accountName)
        .Uint32("DEFAULT_TABLES_FORMAT", 1);

    Register(new TMiniKqlExecutionActor(SelfId(), RequestId_, std::move(ev), false, QueuePath_, GetTransactionCounters(UserCounters_)));
}

void TCreateQueueSchemaActorV2::RegisterMakeDirActor(const TString& workingDir, const TString& dirName) {
    auto ev = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
    auto* trans = ev->Record.MutableTransaction()->MutableModifyScheme();

    trans->SetWorkingDir(workingDir);
    trans->SetOperationType(NKikimrSchemeOp::ESchemeOpMkDir);
    trans->MutableMkDir()->SetName(dirName);

    Register(new TMiniKqlExecutionActor(SelfId(), RequestId_, std::move(ev), false, QueuePath_, GetTransactionCounters(UserCounters_)));
}

void TCreateQueueSchemaActorV2::RequestLeaderTabletId() {
    RLOG_SQS_TRACE("Requesting leader tablet id for path id " << TableWithLeaderPathId_.second);
    THolder<TEvTxUserProxy::TEvNavigate> request(new TEvTxUserProxy::TEvNavigate());
    request->Record.MutableDescribePath()->SetSchemeshardId(TableWithLeaderPathId_.first);
    request->Record.MutableDescribePath()->SetPathId(TableWithLeaderPathId_.second);
    Send(MakeTxProxyID(), std::move(request));
}

void TCreateQueueSchemaActorV2::CreateComponents() {
    switch (CurrentCreationStep_) {
        case ECreateComponentsStep::GetTablesFormatSetting: {
            RequestTablesFormatSettings(QueuePath_.UserName);
            break;
        }
        case ECreateComponentsStep::MakeQueueDir: {
            RegisterMakeDirActor(QueuePath_.GetUserPath(), QueuePath_.QueueName);
            break;
        }
        case ECreateComponentsStep::MakeQueueVersionDir: {
            RegisterMakeDirActor(QueuePath_.GetQueuePath(), VersionName_);
            break;
        }
        case ECreateComponentsStep::MakeShards: {
            for (ui64 shardIdx = 0; shardIdx < RequiredShardsCount_; ++shardIdx) {
                RegisterMakeDirActor(VersionedQueueFullPath_, ToString(shardIdx));
            }
            break;
        }
        case ECreateComponentsStep::MakeTables: {
            for (const TTable& table : RequiredTables_) {
                auto ev = MakeCreateTableEvent(VersionedQueueFullPath_, table, RequiredShardsCount_);
                auto* cmd = ev->Record.MutableTransaction()->MutableModifyScheme()->MutableCreateTable();
                cmd->MutablePartitionConfig()->MutablePipelineConfig()->SetEnableOutOfOrder(Request_.GetEnableOutOfOrderTransactionsExecution());

                const TActorId actorId = Register(new TMiniKqlExecutionActor(
                    SelfId(), RequestId_, std::move(ev), false, QueuePath_, GetTransactionCounters(UserCounters_)));

                if (table.HasLeaderTablet && !CreateTableWithLeaderTabletActorId_) {
                    CreateTableWithLeaderTabletActorId_ = actorId;
                }
            }

            break;
        }
        case ECreateComponentsStep::DiscoverLeaderTabletId: {
            RequestLeaderTabletId();
            break;
        }
        case ECreateComponentsStep::AddQuoterResource: {
            AddRPSQuota();
            break;
        }
        case ECreateComponentsStep::Commit: {
            CommitNewVersion();
            break;
        }
    }
}

STATEFN(TCreateQueueSchemaActorV2::CreateComponentsState) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TSqsEvents::TEvExecuted, OnExecuted);
        hFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, OnDescribeSchemeResult);
        hFunc(NKesus::TEvKesus::TEvAddQuoterResourceResult, HandleAddQuoterResource);
        cFunc(TEvPoisonPill::EventType, PassAway);
    }
}

void TCreateQueueSchemaActorV2::Step() {
    RLOG_SQS_TRACE("Next step. Step: " << (int)CurrentCreationStep_);
    switch (CurrentCreationStep_) {
        case ECreateComponentsStep::GetTablesFormatSetting: {
            CurrentCreationStep_ = ECreateComponentsStep::MakeQueueDir;
            break;
        }
        case ECreateComponentsStep::MakeQueueDir: {
            CurrentCreationStep_ = ECreateComponentsStep::MakeQueueVersionDir;
            break;
        }
        case ECreateComponentsStep::MakeQueueVersionDir: {
            if (TablesFormat_ == 0) {
                if (IsFifo_) {
                    CurrentCreationStep_ = ECreateComponentsStep::MakeTables;
                } else {
                    CurrentCreationStep_ = ECreateComponentsStep::MakeShards;
                }
            } else {
                if (Cfg().GetQuotingConfig().GetEnableQuoting() && Cfg().GetQuotingConfig().HasKesusQuoterConfig()) {
                    CurrentCreationStep_ = ECreateComponentsStep::AddQuoterResource;
                } else {
                    CurrentCreationStep_ = ECreateComponentsStep::Commit;
                }
            }
            break;
        }
        case ECreateComponentsStep::MakeShards: {
            if (++CreatedShardsCount_ != RequiredShardsCount_) {
                return; // do not progress
            }

            CurrentCreationStep_ = ECreateComponentsStep::MakeTables;
            break;
        }

        case ECreateComponentsStep::MakeTables: {
            if (++CreatedTablesCount_ != RequiredTables_.size()) {
                return; // do not progress
            }

            Y_ABORT_UNLESS(TableWithLeaderPathId_.first && TableWithLeaderPathId_.second);
            CurrentCreationStep_ = ECreateComponentsStep::DiscoverLeaderTabletId;
            break;
        }
        case ECreateComponentsStep::DiscoverLeaderTabletId: {
            if (Cfg().GetQuotingConfig().GetEnableQuoting() && Cfg().GetQuotingConfig().HasKesusQuoterConfig()) {
                CurrentCreationStep_ = ECreateComponentsStep::AddQuoterResource;
            } else {
                CurrentCreationStep_ = ECreateComponentsStep::Commit;
            }
            break;
        }
        case ECreateComponentsStep::AddQuoterResource: {
            CurrentCreationStep_ = ECreateComponentsStep::Commit;
            break;
        }
        default: {
            Y_VERIFY_S(false, "incorrect queue creation step: " << CurrentCreationStep_); // unreachable
            break;
        }
    }

    CreateComponents();
}

void TCreateQueueSchemaActorV2::OnExecuted(TSqsEvents::TEvExecuted::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    const auto status = record.GetStatus();
    RLOG_SQS_TRACE("OnExecuted: " << ev->Get()->Record);

    if (ev->Sender == CreateTableWithLeaderTabletActorId_) {
        CreateTableWithLeaderTabletTxId_ = record.GetTxId();
        TableWithLeaderPathId_ = std::make_pair(record.GetSchemeShardTabletId(), record.GetPathId());
        RLOG_SQS_TRACE("Handle executed transaction with leader tablet: " << record);
    }

    // Note:
    // SS finishes transaction immediately if the specified path already exists
    // DO NOT add any special logic based on the result type (except for an error)
    if (IsGoodStatusCode(status)) {
        if (CurrentCreationStep_ == ECreateComponentsStep::GetTablesFormatSetting) {
            const TValue value(TValue::Create(record.GetExecutionEngineEvaluatedResponse()));
            const TValue formatValue = value["tablesFormat"];
            if (formatValue.HaveValue()) {
                TablesFormat_ = static_cast<ui32>(formatValue);
            }
            if (!formatValue.HaveValue() || TablesFormat_ > 1) {
                RLOG_SQS_WARN("Incorrect TablesFormat settings for account "
                    << QueuePath_.UserName << ", responce:" << record);

                auto resp = MakeErrorResponse(NErrors::INTERNAL_FAILURE);
                resp->State = EQueueState::Creating;
                resp->Error = "Incorrect TablesFormat settings for account";

                Send(Sender_, std::move(resp));
                PassAway();
                return;
            }
            RLOG_SQS_DEBUG("Got table format '" << TablesFormat_ << "' for "
                << QueuePath_.UserName << record);
        }

        Step();
    } else {
        RLOG_SQS_WARN("Component creation request execution error: " << record);

        auto resp = MakeErrorResponse(NErrors::INTERNAL_FAILURE);
        resp->State = EQueueState::Creating;

        if (status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::WrongRequest) {
            resp->Error = TStringBuilder() << "Missing account: " << QueuePath_.UserName << ".";
        } else {
            resp->Error = record.GetMiniKQLErrors();
        }

        Send(Sender_, std::move(resp));

        PassAway();
    }
}

void TCreateQueueSchemaActorV2::OnDescribeSchemeResult(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
    RLOG_SQS_TRACE("OnDescribeSchemeResult for leader tablet: " << ev->Get()->GetRecord());
    const auto& pathDescription = ev->Get()->GetRecord().GetPathDescription();

    if (ev->Get()->GetRecord().GetStatus() != NKikimrScheme::StatusSuccess || pathDescription.TablePartitionsSize() == 0 || !pathDescription.GetTablePartitions(0).GetDatashardId()) {
        RLOG_SQS_ERROR("Failed to discover leader: " << ev->Get()->GetRecord());
        auto resp = MakeErrorResponse(NErrors::INTERNAL_FAILURE);
        resp->State = EQueueState::Creating;
        resp->Error = "Failed to discover leader.";

        Send(Sender_, std::move(resp));

        PassAway();
        return;
    }

    LeaderTabletId_ = pathDescription.GetTablePartitions(0).GetDatashardId();

    Step();
}

void TCreateQueueSchemaActorV2::AddRPSQuota() {
    NKikimrKesus::TEvAddQuoterResource cmd;
    auto& res = *cmd.MutableResource();
    res.SetResourcePath(TStringBuilder() << RPS_QUOTA_NAME << "/" << QueuePath_.QueueName);
    res.MutableHierarchicalDRRResourceConfig()->SetMaxUnitsPerSecond(Cfg().GetQuotingConfig().GetKesusQuoterConfig().GetDefaultLimits().GetStdSendMessageRate());
    AddQuoterResourceActor_ = RunAddQuoterResource(TStringBuilder() << QueuePath_.GetUserPath() << "/" << QUOTER_KESUS_NAME, cmd, RequestId_);
}

void TCreateQueueSchemaActorV2::HandleAddQuoterResource(NKesus::TEvKesus::TEvAddQuoterResourceResult::TPtr& ev) {
    AddQuoterResourceActor_ = TActorId();
    auto status = ev->Get()->Record.GetError().GetStatus();
    if (status == Ydb::StatusIds::SUCCESS || status == Ydb::StatusIds::ALREADY_EXISTS) {
        RLOG_SQS_DEBUG("Successfully added quoter resource. Id: " << ev->Get()->Record.GetResourceId());
        Step();
    } else {
        RLOG_SQS_WARN("Failed to add quoter resource: " << ev->Get()->Record);
        auto resp = MakeErrorResponse(status == Ydb::StatusIds::BAD_REQUEST ? NErrors::VALIDATION_ERROR : NErrors::INTERNAL_FAILURE);
        resp->State = EQueueState::Creating;
        resp->Error = "Failed to add quoter resource.";

        Send(Sender_, std::move(resp));

        PassAway();
        return;
    }
}

static const char* const CommitQueueParamsQuery = R"__(
    (
        (let name                   (Parameter 'NAME              (DataType 'Utf8String)))
        (let customName             (Parameter 'CUSTOMNAME        (DataType 'Utf8String)))
        (let folderId               (Parameter 'FOLDERID          (DataType 'Utf8String)))
        (let id                     (Parameter 'ID                (DataType 'String)))
        (let fifo                   (Parameter 'FIFO              (DataType 'Bool)))
        (let contentBasedDeduplication (Parameter 'CONTENT_BASED_DEDUPLICATION (DataType 'Bool)))
        (let now                    (Parameter 'NOW               (DataType 'Uint64)))
        (let createdTimestamp       (Parameter 'CREATED_TIMESTAMP (DataType 'Uint64)))
        (let shards                 (Parameter 'SHARDS            (DataType 'Uint64)))
        (let partitions             (Parameter 'PARTITIONS        (DataType 'Uint64)))
        (let masterTabletId         (Parameter 'MASTER_TABLET_ID  (DataType 'Uint64)))
        (let tablesFormat           (Parameter 'TABLES_FORMAT     (DataType 'Uint32)))
        (let queueIdNumber          (Parameter 'QUEUE_ID_NUMBER   (DataType 'Uint64)))
        (let queueIdNumberHash      (Parameter 'QUEUE_ID_NUMBER_HASH (DataType 'Uint64)))
        (let maxSize                (Parameter 'MAX_SIZE          (DataType 'Uint64)))
        (let delay                  (Parameter 'DELAY             (DataType 'Uint64)))
        (let visibility             (Parameter 'VISIBILITY        (DataType 'Uint64)))
        (let retention              (Parameter 'RETENTION         (DataType 'Uint64)))
        (let receiveMessageWaitTime (Parameter 'RECEIVE_MESSAGE_WAIT_TIME (DataType 'Uint64)))
        (let dlqArn                 (Parameter 'DLQ_TARGET_ARN    (DataType 'Utf8String)))
        (let dlqName                (Parameter 'DLQ_TARGET_NAME   (DataType 'Utf8String)))
        (let maxReceiveCount        (Parameter 'MAX_RECEIVE_COUNT (DataType 'Uint64)))
        (let defaultMaxQueuesCount  (Parameter 'DEFAULT_MAX_QUEUES_COUNT (DataType 'Uint64)))
        (let userName               (Parameter 'USER_NAME         (DataType 'Utf8String)))
        (let tags                   (Parameter 'TAGS              (DataType 'Utf8String)))

        (let attrsTable '%1$s/Attributes)
        (let stateTable '%1$s/State)
        (let settingsTable '%2$s/.Settings)
        (let queuesTable '%2$s/.Queues)
        (let eventsTable '%2$s/.Events)

        (let maxQueuesCountSettingRow '(
            '('Account userName)
            '('Name (Utf8String '"MaxQueuesCount"))))
        (let maxQueuesCountSettingSelect '(
            'Value))
        (let maxQueuesCountSettingRead (SelectRow settingsTable maxQueuesCountSettingRow maxQueuesCountSettingSelect))
        (let maxQueuesCountSetting (Coalesce (If (Exists maxQueuesCountSettingRead) (Cast (Member maxQueuesCountSettingRead 'Value) 'Uint64) defaultMaxQueuesCount) (Uint64 '0)))

        (let queuesRange '(
            '('Account userName userName)
            '('QueueName (Utf8String '"") (Void))))
        (let queues
            (Member (SelectRange queuesTable queuesRange '('QueueName 'CustomQueueName 'Version 'FolderId 'QueueState 'TablesFormat) '()) 'List))
        (let overLimit
            (LessOrEqual maxQueuesCountSetting (Length queues)))

        (let queuesRow '(
            '('Account userName)
            '('QueueName name)))

        (let eventsRow '(
            '('Account userName)
            '('QueueName name)
            '('EventType (Uint64 '1))))

        (let queuesSelect '(
            'QueueState
            'QueueId
            'FifoQueue
            'Shards
            'Partitions
            'Version
            'TablesFormat))
        (let queuesRead (SelectRow queuesTable queuesRow queuesSelect))

        (let existingQueuesWithSameNameAndFolderId
            (If (Equal (Utf8String '"") customName)
                (List (TypeOf queues))
                (Filter queues (lambda '(item) (block '(
                    (return (Coalesce
                        (And
                            (Equal (Member item 'CustomQueueName) customName)
                            (Equal (Member item 'FolderId) folderId))
                        (Bool 'false))))))
                )
            )
        )

        (let existingResourceId (Coalesce
            (Member (ToOptional existingQueuesWithSameNameAndFolderId) 'QueueName)
            (Utf8String '"")
        ))

        (let queueExists
            (Coalesce
                (Or
                    (Or
                        (Equal (Uint64 '1) (Member queuesRead 'QueueState))
                        (Equal (Uint64 '3) (Member queuesRead 'QueueState))
                    )
                    (NotEqual (Utf8String '"") existingResourceId)
                )
                (Bool 'false)))

        (let currentVersion
            (Coalesce
                (Member (ToOptional existingQueuesWithSameNameAndFolderId) 'Version)
                (Member queuesRead 'Version)
                (Uint64 '0)
            )
        )

        (let currentTablesFormat
            (Coalesce
                (Member (ToOptional existingQueuesWithSameNameAndFolderId) 'TablesFormat)
                (Member queuesRead 'TablesFormat)
                tablesFormat
            )
        )

        (let queuesUpdate '(
            '('QueueId id)
            '('CustomQueueName customName)
            '('FolderId folderId)
            '('QueueState (Uint64 '3))
            '('FifoQueue fifo)
            '('DeadLetterQueue (Bool 'false))
            '('CreatedTimestamp createdTimestamp)
            '('Shards shards)
            '('Partitions partitions)
            '('Version queueIdNumber)
            '('DlqName dlqName)
            '('MasterTabletId masterTabletId)
            '('TablesFormat tablesFormat)
            '('Tags tags)))

        (let eventsUpdate '(
            '('CustomQueueName customName)
            '('EventTimestamp now)
            '('FolderId folderId)
            '('Labels tags)))

        (let attrRow '(%3$s))

        (let attrUpdate '(
            '('ContentBasedDeduplication contentBasedDeduplication)
            '('DelaySeconds delay)
            '('FifoQueue fifo)
            '('MaximumMessageSize maxSize)
            '('MessageRetentionPeriod retention)
            '('ReceiveMessageWaitTime receiveMessageWaitTime)
            '('MaxReceiveCount maxReceiveCount)
            '('DlqArn dlqArn)
            '('DlqName dlqName)
            '('VisibilityTimeout visibility)))

        (let willCommit
            (And
                (Not queueExists)
                (Not overLimit)))

        (let stateUpdate '(
                        '('CleanupTimestamp now)
                        '('CreatedTimestamp createdTimestamp)
                        '('LastModifiedTimestamp now)
                        '('InflyCount (Int64 '0))
                        '('MessageCount (Int64 '0))
                        '('RetentionBoundary (Uint64 '0))
                        '('ReadOffset (Uint64 '0))
                        '('WriteOffset (Uint64 '0))
                        '('CleanupVersion (Uint64 '0))))

        (let queueIdNumberAndShardHashes (AsList %4$s))

        (return (Extend
            (AsList
                (SetResult 'exists queueExists)
                (SetResult 'overLimit overLimit)
                (SetResult 'version currentVersion)
                (SetResult 'tablesFormat currentTablesFormat)
                (SetResult 'resourceId existingResourceId)
                (SetResult 'commited willCommit))

            (ListIf queueExists (SetResult 'meta queuesRead))

            (ListIf willCommit (UpdateRow queuesTable queuesRow queuesUpdate))
            (ListIf willCommit (UpdateRow eventsTable eventsRow eventsUpdate))
            (ListIf willCommit (UpdateRow attrsTable attrRow attrUpdate))

            (If (Not willCommit) (AsList (Void))
                (Map (ListFromRange (Uint64 '0) shards) (lambda '(shardOriginal) (block '(
                    (let shard (Cast shardOriginal 'Uint32))
                    (let row '(%5$s))
                    (let update '(
                        '('CleanupTimestamp now)
                        '('CreatedTimestamp createdTimestamp)
                        '('LastModifiedTimestamp now)
                        '('InflyCount (Int64 '0))
                        '('MessageCount (Int64 '0))
                        '('RetentionBoundary (Uint64 '0))
                        '('ReadOffset (Uint64 '0))
                        '('WriteOffset (Uint64 '0))
                        '('CleanupVersion (Uint64 '0))))
                    (return (UpdateRow stateTable row update)))))))
        ))
    )
)__";

TString GetStateTableKeys(ui32 tablesFormat, bool isFifo) {
    if (tablesFormat == 1) {
        if (isFifo) {
            return R"__(
                '('QueueIdNumberHash queueIdNumberHash)
                '('QueueIdNumber queueIdNumber)
            )__";
        }
        return R"__(
            '('QueueIdNumberHash queueIdNumberHash)
            '('QueueIdNumber queueIdNumber)
            '('Shard shard)
        )__";

    }
    return "'('State shardOriginal)";
}

TString GetAttrTableKeys(ui32 tablesFormat) {
    if (tablesFormat == 1) {
        return R"__(
            '('QueueIdNumberHash queueIdNumberHash)
            '('QueueIdNumber queueIdNumber)
        )__";
    }
    return "'('State (Uint64 '0))";
}

TString GetQueueIdAndShardHashesList(ui64 version, ui32 shards) {
    TStringBuilder hashes;
    for (ui32 i = 0; i < shards; ++i) {
        hashes << "(Uint64 '" << GetKeysHash(version, i) << ") ";
    }
    return hashes;
}

void TCreateQueueSchemaActorV2::CommitNewVersion() {
    Become(&TCreateQueueSchemaActorV2::FinalizeAndCommit);

    TString queuePath;
    if (TablesFormat_ == 0) {
        queuePath = Join("/", QueuePath_.GetUserPath(), QueuePath_.QueueName, VersionName_);
    } else {
        queuePath = Join("/", Cfg().GetRoot(), IsFifo_ ? FIFO_TABLES_DIR : STD_TABLES_DIR);
    }

    TString query = Sprintf(
        CommitQueueParamsQuery,
        queuePath.c_str(),
        Cfg().GetRoot().c_str(),
        GetAttrTableKeys(TablesFormat_).c_str(),
        GetQueueIdAndShardHashesList(Version_, RequiredShardsCount_).c_str(),
        GetStateTableKeys(TablesFormat_, IsFifo_).c_str()
    );

    auto ev = MakeExecuteEvent(query);
    auto* trans = ev->Record.MutableTransaction()->MutableMiniKQLTransaction();
    Y_ABORT_UNLESS(TablesFormat_ == 1 || LeaderTabletId_ != 0);
    TInstant createdTimestamp = Request_.HasCreatedTimestamp() ? TInstant::Seconds(Request_.GetCreatedTimestamp()) : QueueCreationTimestamp_;
    TParameters(trans->MutableParams()->MutableProto())
        .Utf8("NAME", QueuePath_.QueueName)
        .Utf8("CUSTOMNAME", CustomQueueName_)
        .Utf8("FOLDERID", FolderId_)
        .String("ID", GeneratedQueueId_)
        .Bool("FIFO", IsFifo_)
        .Bool("CONTENT_BASED_DEDUPLICATION", *ValidatedAttributes_.ContentBasedDeduplication)
        .Uint64("NOW", QueueCreationTimestamp_.MilliSeconds())
        .Uint64("CREATED_TIMESTAMP", createdTimestamp.MilliSeconds())
        .Uint64("SHARDS", RequiredShardsCount_)
        .Uint64("PARTITIONS", Request_.GetPartitions())
        .Uint64("MASTER_TABLET_ID", LeaderTabletId_)
        .Uint32("TABLES_FORMAT", TablesFormat_)
        .Uint64("QUEUE_ID_NUMBER", Version_)
        .Uint64("QUEUE_ID_NUMBER_HASH", GetKeysHash(Version_))
        .Uint64("MAX_SIZE", *ValidatedAttributes_.MaximumMessageSize)
        .Uint64("DELAY", SecondsToMs(*ValidatedAttributes_.DelaySeconds))
        .Uint64("VISIBILITY", SecondsToMs(*ValidatedAttributes_.VisibilityTimeout))
        .Uint64("RETENTION", SecondsToMs(*ValidatedAttributes_.MessageRetentionPeriod))
        .Uint64("RECEIVE_MESSAGE_WAIT_TIME", SecondsToMs(*ValidatedAttributes_.ReceiveMessageWaitTimeSeconds))
        .Utf8("DLQ_TARGET_ARN", ValidatedAttributes_.RedrivePolicy.TargetArn ? *ValidatedAttributes_.RedrivePolicy.TargetArn : "")
        .Utf8("DLQ_TARGET_NAME", ValidatedAttributes_.RedrivePolicy.TargetQueueName ? *ValidatedAttributes_.RedrivePolicy.TargetQueueName :  "")
        .Uint64("MAX_RECEIVE_COUNT", ValidatedAttributes_.RedrivePolicy.MaxReceiveCount ? *ValidatedAttributes_.RedrivePolicy.MaxReceiveCount : 0)
        .Uint64("DEFAULT_MAX_QUEUES_COUNT", Cfg().GetAccountSettingsDefaults().GetMaxQueuesCount())
        .Utf8("USER_NAME", QueuePath_.UserName)
        .Utf8("TAGS", TagsJson_);

    Register(new TMiniKqlExecutionActor(SelfId(), RequestId_, std::move(ev), false, QueuePath_, GetTransactionCounters(UserCounters_)));
}

STATEFN(TCreateQueueSchemaActorV2::FinalizeAndCommit) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TSqsEvents::TEvExecuted, OnCommit);
        cFunc(TEvPoisonPill::EventType, PassAway);
    }
}

void TCreateQueueSchemaActorV2::OnCommit(TSqsEvents::TEvExecuted::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    const auto status = record.GetStatus();

    auto resp = MakeErrorResponse(NErrors::INTERNAL_FAILURE);

    if (status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
        const TValue val(TValue::Create(record.GetExecutionEngineEvaluatedResponse()));
        if (bool(val["commited"])) {
            // a new born queue is here!
            resp->QueueId = GeneratedQueueId_;
            resp->Success = true;
            resp->ErrorClass = nullptr;
        } else {
            // something is off
            if (bool(val["overLimit"])) {
                resp->ErrorClass = &NErrors::OVER_LIMIT;
                resp->Error = "Too many queues.";
             } else if (bool(val["exists"])) {
                if (IsCloudMode_) {
                    ExistingQueueResourceId_ = TString(val["resourceId"]);
                }
                const ui64 currentVersion = ui64(val["version"]);
                const ui32 currentTablesFormat = ui32(val["tablesFormat"]);
                MatchQueueAttributes(currentVersion, currentTablesFormat);
                return;
             } else {
                Y_ABORT_UNLESS(false); // unreachable
             }
        }
    } else {
        resp->Error = "Failed to commit new queue version.";
    }

    Send(Sender_, std::move(resp));
    PassAway();
}

void TCreateQueueSchemaActorV2::MatchQueueAttributes(
    const ui64 currentVersion,
    const ui32 currentTablesFormat
) {
    Become(&TCreateQueueSchemaActorV2::MatchAttributes);

    const TString existingQueueName = IsCloudMode_ ? ExistingQueueResourceId_ : QueuePath_.QueueName;
    TDbQueriesMaker queryMaker(
        Cfg().GetRoot(),
        QueuePath_.UserName,
        existingQueueName,
        currentVersion,
        IsFifo_,
        0,
        currentTablesFormat,
        "", // dlqName
        0, // dlqShard
        0, // dlqVersion
        0 // dlqTablesFormat
    );
    auto query = queryMaker.GetMatchQueueAttributesQuery();

    auto ev = MakeExecuteEvent(query);
    auto* trans = ev->Record.MutableTransaction()->MutableMiniKQLTransaction();
    TParameters(trans->MutableParams()->MutableProto())
        .Uint64("QUEUE_ID_NUMBER", currentVersion)
        .Uint64("QUEUE_ID_NUMBER_HASH", GetKeysHash(currentVersion))
        .Utf8("NAME", existingQueueName)
        .Bool("FIFO", IsFifo_)
        .Uint64("SHARDS", RequiredShardsCount_)
        .Uint64("PARTITIONS", Request_.GetPartitions())
        .Uint64("EXPECTED_VERSION", currentVersion)
        .Uint64("MAX_SIZE", *ValidatedAttributes_.MaximumMessageSize)
        .Uint64("DELAY", SecondsToMs(*ValidatedAttributes_.DelaySeconds))
        .Uint64("VISIBILITY", SecondsToMs(*ValidatedAttributes_.VisibilityTimeout))
        .Uint64("RETENTION", SecondsToMs(*ValidatedAttributes_.MessageRetentionPeriod))
        .Utf8("DLQ_TARGET_NAME", ValidatedAttributes_.RedrivePolicy.TargetQueueName ? *ValidatedAttributes_.RedrivePolicy.TargetQueueName : "")
        .Uint64("MAX_RECEIVE_COUNT", ValidatedAttributes_.RedrivePolicy.MaxReceiveCount ? *ValidatedAttributes_.RedrivePolicy.MaxReceiveCount : 0)
        .Utf8("USER_NAME", QueuePath_.UserName)
        .Utf8("TAGS", TagsJson_);

    Register(new TMiniKqlExecutionActor(SelfId(), RequestId_, std::move(ev), false, QueuePath_, GetTransactionCounters(UserCounters_)));
}

STATEFN(TCreateQueueSchemaActorV2::MatchAttributes) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TSqsEvents::TEvExecuted, OnAttributesMatch);
        cFunc(TEvPoisonPill::EventType, PassAway);
    }
}

void TCreateQueueSchemaActorV2::OnAttributesMatch(TSqsEvents::TEvExecuted::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    const auto status = record.GetStatus();

    auto resp = MakeErrorResponse(NErrors::INTERNAL_FAILURE);

    if (status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
        const TValue val(TValue::Create(record.GetExecutionEngineEvaluatedResponse()));
        if (bool(val["exists"])) {
            resp->AlreadyExists = true;
            resp->ErrorClass = nullptr;
            resp->ExistingQueueResourceId = IsCloudMode_ ? ExistingQueueResourceId_ : QueuePath_.QueueName;
            const bool isSame = bool(val["isSame"]);
            if (isSame || !EnableQueueAttributesValidation_) {
                resp->Success = true;
                resp->ErrorClass = nullptr;
                resp->QueueId = TString(val["id"]);

                if (!isSame) {
                    RLOG_SQS_WARN("Queue attributes do not match for account " << QueuePath_.UserName << " and queue name " << QueuePath_.QueueName);
                }
            } else {
                resp->Error = "queue with specified name already exists and has different attributes.";
                resp->ErrorClass = &NErrors::VALIDATION_ERROR;
            }

            if (TablesFormat_ == 0 && CurrentCreationStep_ == ECreateComponentsStep::DiscoverLeaderTabletId) {
                // call the special version of cleanup actor
                RLOG_SQS_WARN("Removing redundant queue version: " << Version_ << " for queue " <<
                                    QueuePath_.GetQueuePath() << ". Shards: " << RequiredShardsCount_ << " IsFifo: " << IsFifo_);
                Register(new TDeleteQueueSchemaActorV2(QueuePath_, IsFifo_, TablesFormat_, SelfId(), RequestId_, UserCounters_,
                                                           Version_, RequiredShardsCount_, IsFifo_));
            }

        } else {
            resp->Error = "Queue was removed recently.";
            resp->ErrorClass = &NErrors::QUEUE_DELETED_RECENTLY;
            resp->State = EQueueState::Deleting;
        }
    } else {
        resp->Error = "Failed to compare queue attributes.";
    }

    Send(Sender_, std::move(resp));
    PassAway();
}

void TCreateQueueSchemaActorV2::PassAway() {
    if (AddQuoterResourceActor_) {
        Send(AddQuoterResourceActor_, new TEvPoisonPill());
        AddQuoterResourceActor_ = TActorId();
    }
    TActorBootstrapped<TCreateQueueSchemaActorV2>::PassAway();
}

TDeleteQueueSchemaActorV2::TDeleteQueueSchemaActorV2(const TQueuePath& path,
                                                     bool isFifo,
                                                     ui32 tablesFormat,
                                                     const TActorId& sender,
                                                     const TString& requestId,
                                                     TIntrusivePtr<TUserCounters> userCounters)
    : QueuePath_(path)
    , IsFifo_(isFifo)
    , TablesFormat_(tablesFormat)
    , Sender_(sender)
    , DeletionStep_(EDeleting::EraseQueueRecord)
    , RequestId_(requestId)
    , UserCounters_(std::move(userCounters))
{
}

TDeleteQueueSchemaActorV2::TDeleteQueueSchemaActorV2(const TQueuePath& path,
                                                     bool isFifo,
                                                     ui32 tablesFormat,
                                                     const TActorId& sender,
                                                     const TString& requestId,
                                                     TIntrusivePtr<TUserCounters> userCounters,
                                                     const ui64 advisedQueueVersion,
                                                     const ui64 advisedShardCount,
                                                     const bool advisedIsFifoFlag)
    : QueuePath_(path)
    , IsFifo_(isFifo)
    , TablesFormat_(tablesFormat)
    , Sender_(sender)
    , DeletionStep_(tablesFormat == 0 ? EDeleting::RemoveTables : EDeleting::RemoveQueueVersionDirectory)
    , RequestId_(requestId)
    , UserCounters_(std::move(userCounters))
{
    Y_ABORT_UNLESS(advisedQueueVersion > 0);

    Version_ = advisedQueueVersion;

    PrepareCleanupPlan(advisedIsFifoFlag, advisedShardCount);
}

void TDeleteQueueSchemaActorV2::Bootstrap() {
    NextAction();
    Become(&TThis::StateFunc);
}

void TDeleteQueueSchemaActorV2::PrepareCleanupPlan(const bool isFifo, const ui64 shardCount) {
    if (isFifo) {
        Tables_ = GetFifoTables();
    } else {
        Tables_ = GetStandardTableNames(shardCount);

        for (ui64 i = 0; i < shardCount; ++i) {
            Shards_.push_back(i);
        }
    }
}

static TString GetVersionedQueueDir(const TString& baseQueueDir, const ui64 version) {
    if (!version) {
        return baseQueueDir;
    }

    return TString::Join(baseQueueDir, "/v", ToString(version));
}

static const char* EraseQueueRecordQuery = R"__(
    (
        (let name               (Parameter 'NAME (DataType 'Utf8String)))
        (let userName           (Parameter 'USER_NAME (DataType 'Utf8String)))
        (let now                (Parameter 'NOW (DataType 'Uint64)))
        (let queueIdNumber      (Parameter 'QUEUE_ID_NUMBER (DataType 'Uint64)))
        (let queueIdNumberHash  (Parameter 'QUEUE_ID_NUMBER_HASH (DataType 'Uint64)))

        (let queuesTable '%2$s/.Queues)
        (let removedQueuesTable '%2$s/.RemovedQueues)
        (let eventsTable '%2$s/.Events)
        (let stateTable '%3$s/State)

        (let queuesRow '(
            '('Account userName)
            '('QueueName name)))
        (let eventsRow '(
            '('Account userName)
            '('QueueName name)
            '('EventType (Uint64 '0))))

        (let queuesSelect '(
            'QueueState
            'Version
            'FifoQueue
            'Shards
            'CustomQueueName
            'CreatedTimestamp
            'FolderId
            'TablesFormat
            'Tags))
        (let queuesRead (SelectRow queuesTable queuesRow queuesSelect))

        (let currentVersion
            (Coalesce
                (Member queuesRead 'Version)
                (Uint64 '0)
            )
        )

        (let queueCreateTs
            (Coalesce
                (Member queuesRead 'CreatedTimestamp)
                (Uint64 '0)
            )
        )

        (let fifoQueue
            (Coalesce
                (Member queuesRead 'FifoQueue)
                (Bool 'false)
            )
        )

        (let shards
            (Coalesce
                (Member queuesRead 'Shards)
                (Uint64 '0)
            )
        )

        (let folderId
            (Coalesce
                (Member queuesRead 'FolderId)
                (Utf8String '"")
            )
        )

        (let customName
            (Coalesce
                (Member queuesRead 'CustomQueueName)
                (Utf8String '"")
            )
        )

        (let tablesFormat
            (Coalesce
                (Member queuesRead 'TablesFormat)
                (Uint32 '0)
            )
        )

        (let queueTags
            (Coalesce
                (Member queuesRead 'Tags)
                (Utf8String '"{}")
            )
        )

        (let removedQueueRow '(
            '('RemoveTimestamp now)
            '('QueueIdNumber currentVersion)))

        (let removedQueueUpdate '(
            '('Account userName)
            '('QueueName name)
            '('FifoQueue fifoQueue)
            '('Shards (Cast shards 'Uint32))
            '('CustomQueueName customName)
            '('FolderId folderId)
            '('TablesFormat tablesFormat)))

        (let eventTs (Max now (Add queueCreateTs (Uint64 '2))))

        (let queueExists
            (Coalesce
                (And
                    (Equal currentVersion queueIdNumber)
                    (Or
                        (Equal (Uint64 '1) (Member queuesRead 'QueueState))
                        (Equal (Uint64 '3) (Member queuesRead 'QueueState))
                    )
                )
                (Bool 'false)))

        (let eventsUpdate '(
            '('CustomQueueName customName)
            '('EventTimestamp eventTs)
            '('FolderId folderId)
            '('Labels queueTags)))

        (return (Extend
            (AsList
                (SetResult 'exists queueExists)
                (SetResult 'version currentVersion)
                (SetResult 'fields queuesRead)
                (If queueExists (UpdateRow eventsTable eventsRow eventsUpdate) (Void))
                (If queueExists (UpdateRow removedQueuesTable removedQueueRow removedQueueUpdate) (Void))
                (If queueExists (EraseRow queuesTable queuesRow) (Void))
            )

                (If queueExists
                    (Map (ListFromRange (Uint64 '0) shards) (lambda '(shardOriginal) (block '(
                        (let shard (Cast shardOriginal 'Uint32))

                        (let stateRow '(%4$s))
                        (return (EraseRow stateTable stateRow))
                    ))))
                    (AsList (Void))
                )
        ))
    )
)__";

void TDeleteQueueSchemaActorV2::NextAction() {
    switch (DeletionStep_) {
        case EDeleting::EraseQueueRecord: {
            TString queueStateDir = QueuePath_.GetVersionedQueuePath();
            if (TablesFormat_ == 1) {
                queueStateDir = Join("/", Cfg().GetRoot(), IsFifo_ ? FIFO_TABLES_DIR : STD_TABLES_DIR);
            }

            auto ev = MakeExecuteEvent(Sprintf(
                EraseQueueRecordQuery,
                QueuePath_.GetUserPath().c_str(),
                Cfg().GetRoot().c_str(),
                queueStateDir.c_str(),
                GetStateTableKeys(TablesFormat_, IsFifo_).c_str()
            ));
            auto* trans = ev->Record.MutableTransaction()->MutableMiniKQLTransaction();
            auto nowMs = TInstant::Now().MilliSeconds();
            TParameters(trans->MutableParams()->MutableProto())
                .Utf8("NAME", QueuePath_.QueueName)
                .Uint64("QUEUE_ID_NUMBER", QueuePath_.Version)
                .Uint64("QUEUE_ID_NUMBER_HASH", GetKeysHash(QueuePath_.Version))
                .Utf8("USER_NAME", QueuePath_.UserName)
                .Uint64("NOW", nowMs);

            Register(new TMiniKqlExecutionActor(SelfId(), RequestId_, std::move(ev), false, QueuePath_, GetTransactionCounters(UserCounters_)));
            break;
        }
        case EDeleting::RemoveTables: {
            Y_ABORT_UNLESS(!Tables_.empty());

            Register(new TMiniKqlExecutionActor(
                SelfId(), RequestId_, MakeDeleteTableEvent(GetVersionedQueueDir(QueuePath_, Version_), Tables_.back()), false, QueuePath_, GetTransactionCounters(UserCounters_))
            );
            break;
        }
        case EDeleting::RemoveShards: {
            Register(new TMiniKqlExecutionActor(
                SelfId(), RequestId_, MakeRemoveDirectoryEvent(GetVersionedQueueDir(QueuePath_, Version_), ToString(Shards_.back())), false, QueuePath_, GetTransactionCounters(UserCounters_))
            );
            break;
        }
        case EDeleting::RemoveQueueVersionDirectory: {
            Register(new TMiniKqlExecutionActor(
                SelfId(), RequestId_, MakeRemoveDirectoryEvent(QueuePath_, TString::Join("v", ToString(Version_))), false, QueuePath_, GetTransactionCounters(UserCounters_))
            );
            break;
        }
        case EDeleting::RemoveQueueDirectory: {
            // this may silently fail for versioned queues
            Register(new TMiniKqlExecutionActor(
                SelfId(), RequestId_, MakeRemoveDirectoryEvent(QueuePath_.GetUserPath(), QueuePath_.QueueName), false, QueuePath_, GetTransactionCounters(UserCounters_))
            );
            break;
        }
        case EDeleting::DeleteQuoterResource: {
            DeleteRPSQuota();
            break;
        }
        case EDeleting::Finish: {
            Send(Sender_, MakeHolder<TSqsEvents::TEvQueueDeleted>(QueuePath_, true));
            PassAway();
            break;
        }
    }
}

void TDeleteQueueSchemaActorV2::DoSuccessOperation() {
    switch (DeletionStep_) {
        case EDeleting::EraseQueueRecord: {
            if (TablesFormat_ == 0) {
                DeletionStep_ = EDeleting::RemoveTables;
            } else {
                DeletionStep_ = EDeleting::RemoveQueueVersionDirectory;
            }
            break;
        }
        case EDeleting::RemoveTables: {
            Tables_.pop_back();

            if (Tables_.empty()) {
                if (Shards_.empty()) {
                    DeletionStep_ = Version_ ? EDeleting::RemoveQueueVersionDirectory : EDeleting::RemoveQueueDirectory;
                } else {
                    DeletionStep_ = EDeleting::RemoveShards;
                }
            }
            break;
        }
        case EDeleting::RemoveShards: {
            Shards_.pop_back();

            if (Shards_.empty()) {
                DeletionStep_ = Version_ ? EDeleting::RemoveQueueVersionDirectory : EDeleting::RemoveQueueDirectory;
            }
            break;
        }
        case EDeleting::RemoveQueueVersionDirectory: {
            DeletionStep_ = EDeleting::RemoveQueueDirectory;
            break;
        }
        case EDeleting::RemoveQueueDirectory: {
            if (Cfg().GetQuotingConfig().GetEnableQuoting() && Cfg().GetQuotingConfig().HasKesusQuoterConfig()) {
                DeletionStep_ = EDeleting::DeleteQuoterResource;
            } else {
                DeletionStep_ = EDeleting::Finish;
            }
            break;
        }
        case EDeleting::DeleteQuoterResource: {
            DeletionStep_ = EDeleting::Finish;
            break;
        }
        default: {
            Y_VERIFY_S(false, "incorrect queue deletion step: " << DeletionStep_); // unreachable
        }
    }

    NextAction();
}

void TDeleteQueueSchemaActorV2::HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    if (IsGoodStatusCode(record.GetStatus())) {
        if (DeletionStep_ == EDeleting::EraseQueueRecord) {
            const TValue val(TValue::Create(record.GetExecutionEngineEvaluatedResponse()));
            if (!bool(val["exists"])) {
                Send(Sender_,
                    MakeHolder<TSqsEvents::TEvQueueDeleted>(QueuePath_, false, "Queue does not exist."));
                PassAway();
                return;
            } else {
                Version_ = ui64(val["version"]);

                PrepareCleanupPlan(bool(val["fields"]["FifoQueue"]), ui64(val["fields"]["Shards"]));
            }
        }

        DoSuccessOperation();
    } else {
        RLOG_SQS_WARN("request execution error: " << record);

        if (DeletionStep_ == EDeleting::EraseQueueRecord) {
            Send(Sender_,
                     MakeHolder<TSqsEvents::TEvQueueDeleted>(QueuePath_, false, "Failed to erase queue record."));
            PassAway();
            return;
        }

        // we don't really care if some components are already deleted
        DoSuccessOperation();
    }
}

void TDeleteQueueSchemaActorV2::DeleteRPSQuota() {
    NKikimrKesus::TEvDeleteQuoterResource cmd;
    cmd.SetResourcePath(TStringBuilder() << RPS_QUOTA_NAME << "/" << QueuePath_.QueueName);
    DeleteQuoterResourceActor_ = RunDeleteQuoterResource(TStringBuilder() << QueuePath_.GetUserPath() << "/" << QUOTER_KESUS_NAME, cmd, RequestId_);
}

void TDeleteQueueSchemaActorV2::HandleDeleteQuoterResource(NKesus::TEvKesus::TEvDeleteQuoterResourceResult::TPtr& ev) {
    DeleteQuoterResourceActor_ = TActorId();
    auto status = ev->Get()->Record.GetError().GetStatus();
    if (status == Ydb::StatusIds::SUCCESS || status == Ydb::StatusIds::NOT_FOUND) {
        RLOG_SQS_DEBUG("Successfully deleted quoter resource");

        DoSuccessOperation();
    } else {
        RLOG_SQS_WARN("Failed to delete quoter resource: " << ev->Get()->Record);

        Send(Sender_,
                 MakeHolder<TSqsEvents::TEvQueueDeleted>(QueuePath_, false, "Failed to delete RPS quoter resource."));
        PassAway();
    }
}

void TDeleteQueueSchemaActorV2::PassAway() {
    if (DeleteQuoterResourceActor_) {
        Send(DeleteQuoterResourceActor_, new TEvPoisonPill());
        DeleteQuoterResourceActor_ = TActorId();
    }
    TActorBootstrapped<TDeleteQueueSchemaActorV2>::PassAway();
}

} // namespace NKikimr::NSQS
