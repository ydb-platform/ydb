#include "cfg.h"
#include "log.h"
#include "migration.h"
#include "executor.h"

#include <ydb/core/tx/scheme_cache/scheme_cache.h> 
#include <ydb/core/ymq/base/debug_info.h> 
#include <ydb/core/ymq/queues/common/queries.h> 

#include <util/string/builder.h>
#include <util/string/join.h>

#include <limits>

namespace NKikimr::NSQS {

class TAddColumnActor : public TActorBootstrapped<TAddColumnActor> {
public:
    TAddColumnActor(const TString& userName,
                    const TString& queueName,
                    const TActorId& parent,
                    const TActorId& schemeCache,
                    TIntrusivePtr<TQueueCounters> counters,
                    const TString& tablePath,
                    const TString& columnName,
                    NScheme::TTypeId columnType);
    virtual ~TAddColumnActor() = default;

    void Bootstrap();

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SQS_QUEUE_MIGRATION_ACTOR;
    }

private:
    STATEFN(StateFunc);

    void HandleTableInfo(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
    void HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev);

    void SendReplyAndDie(bool ok);

    void GetTableInfo();

    void CheckAndAddColumn(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry);
    THolder<TEvTxUserProxy::TEvProposeTransaction> MakeAlterTableEvent();
    void OnTableAltered(const TSqsEvents::TEvExecuted::TRecord& ev);

private:
    const TString UserName;
    const TString QueueName;
    const TString TablePath;
    const TString ColumnName;
    const NScheme::TTypeId ColumnType;
    const TActorId Parent;
    const TActorId SchemeCache;
    TIntrusivePtr<TQueueCounters> Counters;
};

TAddColumnActor::TAddColumnActor(const TString& userName,
                                 const TString& queueName,
                                 const TActorId& parent,
                                 const TActorId& schemeCache,
                                 TIntrusivePtr<TQueueCounters> counters,
                                 const TString& tablePath,
                                 const TString& columnName,
                                 NScheme::TTypeId columnType)
    : UserName(userName)
    , QueueName(queueName)
    , TablePath(tablePath)
    , ColumnName(columnName)
    , ColumnType(columnType)
    , Parent(parent)
    , SchemeCache(schemeCache)
    , Counters(counters)
{
}

STATEFN(TAddColumnActor::StateFunc) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleTableInfo);
        hFunc(TSqsEvents::TEvExecuted, HandleExecuted);
    }
}

void TAddColumnActor::HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev) {
    ev->Get()->Call();
}

void TAddColumnActor::Bootstrap() {
    Become(&TAddColumnActor::StateFunc);

    GetTableInfo();
}

void TAddColumnActor::SendReplyAndDie(bool ok) {
    Send(Parent, new TSqsEvents::TEvMigrationDone(ok));
    PassAway();
}

void TAddColumnActor::GetTableInfo() {
    auto schemeCacheRequest = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    schemeCacheRequest->ResultSet.emplace_back();
    auto& entry = schemeCacheRequest->ResultSet.back();
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
    entry.Path = SplitPath(TablePath);
    Send(SchemeCache, new TEvTxProxySchemeCache::TEvNavigateKeySet(schemeCacheRequest.Release()));
}

static TString ToString(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry) {
    TStringBuilder s;
    s << "{ TableId: " << entry.TableId
      << " Columns: [";

    for (const auto& [id, col] : entry.Columns) {
        s << " { Name: \"" << col.Name << "\""
          << " Type: " << NScheme::TypeName(col.PType)
          << " }";
    }
    s << " ] }";
    return std::move(s);
}

void TAddColumnActor::HandleTableInfo(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    const NSchemeCache::TSchemeCacheNavigate* navigate = ev->Get()->Request.Get();
    Y_VERIFY(navigate->ResultSet.size() == 1);
    const auto& entry = navigate->ResultSet.front();
    if (navigate->ErrorCount > 0) {
        LOG_SQS_ERROR("Queue " << TLogQueueName(UserName, QueueName)
                               << " migration. Failed to get table \"" << TablePath << "\" info: "
                               << entry.Status);
        SendReplyAndDie(false);
    } else {
        LOG_SQS_DEBUG("Queue " << TLogQueueName(UserName, QueueName)
                               << " migration. Table \"" << TablePath << "\" info: "
                               << ToString(entry));
        CheckAndAddColumn(entry);
    }
}

THolder<TEvTxUserProxy::TEvProposeTransaction> TAddColumnActor::MakeAlterTableEvent() {
    auto ev = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
    // Transaction info
    auto* trans = ev->Record.MutableTransaction()->MutableModifyScheme();
    trans->SetOperationType(NKikimrSchemeOp::ESchemeOpAlterTable);

    auto* info = trans->MutableAlterTable();
    {
        const size_t lastSlashPos = TablePath.rfind('/');
        trans->SetWorkingDir(TablePath.substr(0, lastSlashPos));
        info->SetName(TablePath.substr(lastSlashPos + 1));
    }

    auto* col = info->AddColumns();
    col->SetName(ColumnName);
    col->SetType(NScheme::TypeName(ColumnType));

    return ev;
}

void TAddColumnActor::CheckAndAddColumn(const NSchemeCache::TSchemeCacheNavigate::TEntry& entry) {
    for (const auto& [id, col] : entry.Columns) {
        if (col.Name == ColumnName) {
            LOG_SQS_DEBUG("Queue " << TLogQueueName(UserName, QueueName)
                                   << " migration. Found column \"" << ColumnName << "\" in table \"" << TablePath << "\" info. Do nothing");
            SendReplyAndDie(true);
            return;
        }
    }

    // Column was not found. Start altering table
    auto alterTableEvent = MakeAlterTableEvent();
    LOG_SQS_INFO("Queue " << TLogQueueName(UserName, QueueName)
                          << " migration. Adding column \"" << ColumnName << "\" to table \""
                          << TablePath << "\": " << alterTableEvent->Record);
    auto transactionCounters = Counters->GetTransactionCounters();
    Register(new TMiniKqlExecutionActor(
                                        SelfId(),
                                        "",
                                        std::move(alterTableEvent),
                                        false,
                                        TQueuePath(Cfg().GetRoot(), UserName, QueueName),
                                        transactionCounters,
                                        [this](const TSqsEvents::TEvExecuted::TRecord& ev) { OnTableAltered(ev); })
                 );
}

void TAddColumnActor::OnTableAltered(const TSqsEvents::TEvExecuted::TRecord& ev) {
    if (ev.GetStatus() == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
        LOG_SQS_INFO("Queue " << TLogQueueName(UserName, QueueName)
                              << " migration. Added column \"" << ColumnName << "\" to table \""
                              << TablePath << "\": " << ev);
        SendReplyAndDie(true);
    } else {
        LOG_SQS_ERROR("Queue " << TLogQueueName(UserName, QueueName)
                               << " migration. Failed to add column \"" << ColumnName << "\" to table \""
                               << TablePath << "\": " << ev);
        SendReplyAndDie(false);
    }
}


TQueueMigrationActor::TQueueMigrationActor(const TString& userName, const TString& queueName, const TActorId& queueLeader, const TActorId& schemeCache, TIntrusivePtr<TQueueCounters> counters, TDuration waitBeforeMigration)
    : UserName(userName)
    , QueueName(queueName)
    , QueueLeader(queueLeader)
    , SchemeCache(schemeCache)
    , Counters(std::move(counters))
    , WaitBeforeMigration(waitBeforeMigration)
{
    DebugInfo->QueueMigrationActors.emplace(TStringBuilder() << TLogQueueName(QueueName, UserName), this);
}

TQueueMigrationActor::~TQueueMigrationActor() {
    DebugInfo->QueueMigrationActors.EraseKeyValue(TStringBuilder() << TLogQueueName(QueueName, UserName), this);
}

void TQueueMigrationActor::Bootstrap() {
    Become(&TQueueMigrationActor::StateFunc);

    if (!Cfg().GetDoAutomaticMigration()) {
        LOG_SQS_DEBUG("Queue " << TLogQueueName(UserName, QueueName)
                               << " migration. Migration is turned off. Skipping it");
        SendReplyAndDie(true);
    }

    if (WaitBeforeMigration) {
        Schedule(WaitBeforeMigration, new TEvWakeup());
    } else {
        GetQueueParams();
    }
}

STATEFN(TQueueMigrationActor::StateFunc) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvWakeup, HandleWakeup);
        hFunc(TSqsEvents::TEvExecuted, HandleExecuted);
        hFunc(TSqsEvents::TEvMigrationDone, HandleMigrationDone);
    }
}

void TQueueMigrationActor::SendReplyAndDie(bool ok) {
    if (ok) {
        LOG_SQS_DEBUG("Queue " << TLogQueueName(UserName, QueueName)
                               << " migration. Migration is done successfully");
    } else {
        LOG_SQS_DEBUG("Queue " << TLogQueueName(UserName, QueueName)
                               << " migration. Migration failed");
    }
    Send(QueueLeader, new TSqsEvents::TEvMigrationDone(ok));
    PassAway();
}

void TQueueMigrationActor::HandleWakeup([[maybe_unused]] TEvWakeup::TPtr& ev) {
    GetQueueParams();
}

void TQueueMigrationActor::HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev) {
    ev->Get()->Call();
}

void TQueueMigrationActor::HandleMigrationDone(TSqsEvents::TEvMigrationDone::TPtr& ev) {
    // All logging was done in child actor if something happened
    if (!ev->Get()->Success) {
        Answer = false;
    }
    if (--WaitChildrenCount == 0) { // Wait all children even if there was an error to prevent heavy concurrent modifications
        SendReplyAndDie(Answer);
    }
}

void TQueueMigrationActor::GetQueueParams() {
    TExecutorBuilder(SelfId(), "")
        .User(UserName)
        .Queue(QueueName)
        .RetryOnTimeout()
        .Text(Sprintf(GetQueueParamsQuery, Cfg().GetRoot().c_str()))
        .OnExecuted([this](const TSqsEvents::TEvExecuted::TRecord& ev) { OnQueueParams(ev); })
        .Counters(Counters)
        .Params()
            .Utf8("NAME", QueueName)
            .Utf8("USER_NAME", UserName)
        .ParentBuilder().StartExecutorActor();
}

void TQueueMigrationActor::OnQueueParams(const TSqsEvents::TEvExecuted::TRecord& ev) {
    if (ev.GetStatus() == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
        using NKikimr::NClient::TValue;
        const TValue val(TValue::Create(ev.GetExecutionEngineEvaluatedResponse()));

        if (bool(val["exists"])) {
            const auto data(val["queue"]);
            ShardsCount = data["Shards"];
            if (data["Version"].HaveValue()) {
                QueueVersion = data["Version"];
            }
            IsFifoQueue = data["FifoQueue"];

            LOG_SQS_DEBUG("Queue " << TLogQueueName(UserName, QueueName)
                                   << " migration. Got queue params: { ShardsCount: " << ShardsCount << " QueueVersion: " << QueueVersion << " IsFifoQueue: " << IsFifoQueue << " }");

            StartAltering();

            if (WaitChildrenCount == 0) {
                SendReplyAndDie(true);
            }
        } else {
            LOG_SQS_WARN("Queue " << TLogQueueName(UserName, QueueName)
                                  << " migration. Queue doen't exist. Do nothing");
            // Queue is deleting.
            // Do nothing.
            SendReplyAndDie(true);
        }
    } else {
        LOG_SQS_ERROR("Queue " << TLogQueueName(UserName, QueueName)
                               << " migration. Failed to get queue params: " << ev);
        SendReplyAndDie(false);
    }
}

void TQueueMigrationActor::RegisterAndWaitChildMigrationActor(IActor* child) {
    Register(child);
    ++WaitChildrenCount;
}

TString TQueueMigrationActor::GetTablePath(const ui64 shard, const TString& tableName) {
    const TString queuePath = TQueuePath(Cfg().GetRoot(), UserName, QueueName, QueueVersion).GetVersionedQueuePath();
    TStringBuilder path;
    path << queuePath;
    if (shard != std::numeric_limits<ui64>::max()) {
        path << "/" << shard;
    }
    path << "/" << tableName;
    return std::move(path);
}

void TQueueMigrationActor::CheckAddColumn(const ui64 shard, const TString& tableName, const TString& columnName, NScheme::TTypeId type) {
    RegisterAndWaitChildMigrationActor(new TAddColumnActor(UserName, QueueName,
                                                           SelfId(), SchemeCache,
                                                           Counters,
                                                           GetTablePath(shard, tableName), columnName, type));
}

void TQueueMigrationActor::CheckAddColumn(const TString& tableName, const TString& columnName, NScheme::TTypeId type) {
    CheckAddColumn(std::numeric_limits<ui64>::max(), tableName, columnName, type);
}

void TQueueMigrationActor::StartAltering() {
    CheckAddColumn("Attributes", "DlqName", NScheme::NTypeIds::Utf8);
    CheckAddColumn("Attributes", "DlqArn", NScheme::NTypeIds::Utf8);
    CheckAddColumn("Attributes", "MaxReceiveCount", NScheme::NTypeIds::Uint64);
    CheckAddColumn("Attributes", "ShowDetailedCountersDeadline", NScheme::NTypeIds::Uint64);

    CheckAddColumn("State", "InflyVersion", NScheme::NTypeIds::Uint64);
    if (!IsFifoQueue) {
        for (ui64 shard = 0; shard < ShardsCount; ++shard) {
            CheckAddColumn(shard, "Infly", "DelayDeadline", NScheme::NTypeIds::Uint64);
        }
    }
}

} // namespace NKikimr::NSQS
