#include "cfg.h"
#include "executor.h"
#include "log.h"
#include "params.h"
#include "schema.h"
#include <ydb/core/ymq/base/constants.h>
#include <ydb/core/ymq/base/limits.h>
#include <ydb/core/ymq/queues/fifo/schema.h>
#include <ydb/core/ymq/queues/std/schema.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/client/minikql_compile/mkql_compile_service.h>
#include <ydb/public/lib/value/value.h>

#include <util/generic/guid.h>
#include <util/generic/utility.h>
#include <util/string/ascii.h>
#include <util/string/cast.h>
#include <util/string/join.h>

using NKikimr::NClient::TValue;

namespace NKikimr::NSQS {

extern const TString QUOTER_KESUS_NAME = ".Quoter";
extern const TString RPS_QUOTA_NAME = "RPSQuota";

namespace {

static const char* const GetNextAtomicValueQuery = R"__(
    (
        (let counterTable '%1$s/.AtomicCounter)
        (let counterRow '(
            '('counter_key (Uint64 '0))))
        (let counterSelect '(
            'value))
        (let counterRead
            (SelectRow counterTable counterRow counterSelect))
        (let newValue
            (Add (Member counterRead 'value) (Uint64 '1)))
        (let counterUpdate '(
            '('value newValue)))
        (return (Extend
            (AsList (SetResult 'value newValue))
            (AsList (UpdateRow counterTable counterRow counterUpdate))
        ))
    )
)__";

static bool SuccessStatusCode(ui32 code) {
    switch (NTxProxy::TResultStatus::EStatus(code)) {
        case NTxProxy::TResultStatus::EStatus::ExecComplete:
        case NTxProxy::TResultStatus::EStatus::ExecAlready:
            return true;
        default:
            return false;
    }
}

static void SetCompactionPolicyForSmallTable(NKikimrSchemeOp::TPartitionConfig& partitionConfig) {
    auto& policyPb = *partitionConfig.MutableCompactionPolicy();
    policyPb.SetInMemSizeToSnapshot(100000);
    policyPb.SetInMemStepsToSnapshot(300);
    policyPb.SetInMemForceStepsToSnapshot(500);
    policyPb.SetInMemForceSizeToSnapshot(400000);
    policyPb.SetInMemCompactionBrokerQueue(0);
    policyPb.SetReadAheadHiThreshold(64 * 1024 * 1024);
    policyPb.SetReadAheadLoThreshold(16 * 1024 * 1024);
    policyPb.SetMinDataPageSize(7168);
}

static void SetCompactionPolicyForTimestampOrdering(NKikimrSchemeOp::TPartitionConfig& partitionConfig) {
    const bool enableCompression = false;

    auto& policyPb = *partitionConfig.MutableCompactionPolicy();
    policyPb.SetInMemSizeToSnapshot(1*1024*1024);
    policyPb.SetInMemStepsToSnapshot(100000);
    policyPb.SetInMemForceStepsToSnapshot(100000);
    policyPb.SetInMemForceSizeToSnapshot(4*1024*1024);
    policyPb.SetInMemCompactionBrokerQueue(0);
    policyPb.SetReadAheadHiThreshold(64 * 1024 * 1024);
    policyPb.SetReadAheadLoThreshold(16 * 1024 * 1024);
    policyPb.SetMinDataPageSize(enableCompression ? 256*1024 : 63*1024);
    policyPb.SetSnapBrokerQueue(0);
    policyPb.SetBackupBrokerQueue(0);

    auto& g1 = *policyPb.AddGeneration();
    g1.SetGenerationId(0);
    g1.SetSizeToCompact(100 * 1024 * 1024);
    g1.SetCountToCompact(40);
    g1.SetForceCountToCompact(60);
    g1.SetForceSizeToCompact(140 * 1024 * 1024);
    g1.SetCompactionBrokerQueue(1);
    g1.SetKeepInCache(true);

    auto& g2 = *policyPb.AddGeneration();
    g2.SetGenerationId(1);
    g2.SetSizeToCompact(500ull * 1024 * 1024);
    g2.SetCountToCompact(30);
    g2.SetForceCountToCompact(60);
    g2.SetForceSizeToCompact(700ull * 1024 * 1024);
    g2.SetCompactionBrokerQueue(2);
    g2.SetKeepInCache(false);
}

static void SetOnePartitionPerShardSettings(NKikimrSchemeOp::TTableDescription& desc, size_t queueShardsCount) {
    for (size_t boundary = 1; boundary < queueShardsCount; ++boundary) {
        NKikimrSchemeOp::TSplitBoundary* splitBoundarySetting = desc.AddSplitBoundary();
        splitBoundarySetting->MutableKeyPrefix()->AddTuple()->MutableOptional()->SetUint64(boundary);
    }
}

} // namespace

THolder<TEvTxUserProxy::TEvProposeTransaction>
    MakeExecuteEvent(const TString& query)
{
    auto ev = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
    auto* trans = ev->Record.MutableTransaction()->MutableMiniKQLTransaction();
    trans->SetMode(NKikimrTxUserProxy::TMiniKQLTransaction::COMPILE_AND_EXEC);
    trans->SetFlatMKQL(true);
    trans->MutableProgram()->SetText(query);

    return ev;
}

THolder<TEvTxUserProxy::TEvProposeTransaction>
    MakeCreateTableEvent(const TString& root,
                         const TTable& table,
                         size_t queueShardsCount)
{
    auto ev = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
    // Transaction info
    auto* trans = ev->Record.MutableTransaction()->MutableModifyScheme();

    if (table.Shard != -1) {
        trans->SetWorkingDir(TString::Join(root, "/", ToString(table.Shard)));
    } else {
        trans->SetWorkingDir(root);
    }
    trans->SetOperationType(NKikimrSchemeOp::ESchemeOpCreateTable);
    // Table info
    auto* desc = trans->MutableCreateTable();
    desc->SetName(table.Name);
    // Columns info
    for (const auto& col : table.Columns) {
        if (col.Partitions && !table.EnableAutosplit) {
            desc->SetUniformPartitionsCount(col.Partitions);
        }
        if (col.Key) {
            desc->AddKeyColumnNames(col.Name);
        }

        auto* item = desc->AddColumns();
        item->SetName(col.Name);
        item->SetType(NScheme::TypeName(col.TypeId));
    }

    if (table.InMemory) {
        auto* def = desc->MutablePartitionConfig()->AddColumnFamilies();
        def->SetId(0);
        def->SetColumnCache(NKikimrSchemeOp::ColumnCacheEver);
    }

    if (table.Sequential) {
        SetCompactionPolicyForTimestampOrdering(*desc->MutablePartitionConfig());
    } else if (table.Small) {
        SetCompactionPolicyForSmallTable(*desc->MutablePartitionConfig());
    }
    if (table.OnePartitionPerShard) {
        Y_ABORT_UNLESS(queueShardsCount > 0);
        SetOnePartitionPerShardSettings(*desc, queueShardsCount);
    }
    if (table.EnableAutosplit) {
        auto* partitioningPolicy = desc->MutablePartitionConfig()->MutablePartitioningPolicy();
        Y_ABORT_UNLESS(table.SizeToSplit > 0);
        partitioningPolicy->SetSizeToSplit(table.SizeToSplit);
        partitioningPolicy->SetMinPartitionsCount(1);
        partitioningPolicy->SetMaxPartitionsCount(MAX_PARTITIONS_COUNT);
    }

    return ev;
}

THolder<TEvTxUserProxy::TEvProposeTransaction>
    MakeDeleteTableEvent(const TString& root,
                         const TTable& table)
{
    auto ev = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
    // Transaction info
    auto* trans = ev->Record.MutableTransaction()->MutableModifyScheme();
    if (table.Shard == -1) {
        trans->SetWorkingDir(root);
    } else {
        trans->SetWorkingDir(root + "/" + ToString(table.Shard));
    }
    trans->SetOperationType(NKikimrSchemeOp::ESchemeOpDropTable);
    trans->MutableDrop()->SetName(table.Name);

    return ev;
}

THolder<TEvTxUserProxy::TEvProposeTransaction>
    MakeRemoveDirectoryEvent(const TString& root, const TString& name)
{
    auto ev = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
    // Transaction info
    auto* trans = ev->Record.MutableTransaction()->MutableModifyScheme();
    trans->SetWorkingDir(root);
    trans->SetOperationType(NKikimrSchemeOp::ESchemeOpRmDir);
    trans->MutableDrop()->SetName(name);

    return ev;
}

THolder<TEvTxUserProxy::TEvProposeTransaction>
    MakeCreateKesusEvent(const TString& root,
                         const TString& kesusName)
{
    auto ev = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();

    // Transaction info
    auto* trans = ev->Record.MutableTransaction()->MutableModifyScheme();
    trans->SetWorkingDir(root);
    trans->SetOperationType(NKikimrSchemeOp::ESchemeOpCreateKesus);

    // Kesus info
    auto* kesusDesc = trans->MutableKesus();
    kesusDesc->SetName(kesusName);

    return ev;
}

THolder<TEvTxUserProxy::TEvProposeTransaction>
    MakeRemoveKesusEvent(const TString& root,
                         const TString& kesusName)
{
    auto ev = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();

    // Transaction info
    auto* trans = ev->Record.MutableTransaction()->MutableModifyScheme();
    trans->SetWorkingDir(root);
    trans->SetOperationType(NKikimrSchemeOp::ESchemeOpDropKesus);
    auto* drop = trans->MutableDrop();
    drop->SetName(kesusName);

    return ev;
}

TCreateUserSchemaActor::TCreateUserSchemaActor(const TString& root,
                                               const TString& userName,
                                               const TActorId& sender,
                                               const TString& requestId,
                                               TIntrusivePtr<TUserCounters> userCounters)
    : Root_(root)
    , UserName_(userName)
    , Sender_(sender)
    , SI_(static_cast<int>(ECreating::MakeDirectory))
    , RequestId_(requestId)
    , UserCounters_(std::move(userCounters))
{
}

TCreateUserSchemaActor::~TCreateUserSchemaActor() = default;

void TCreateUserSchemaActor::Bootstrap() {
    Process();
    Become(&TThis::StateFunc);
}

void TCreateUserSchemaActor::NextAction() {
    SI_++;

    if (!Cfg().GetQuotingConfig().GetEnableQuoting() || !Cfg().GetQuotingConfig().HasKesusQuoterConfig()) {
        while (ECreating(SI_) == ECreating::Quoter || ECreating(SI_) == ECreating::RPSQuota) {
            SI_++;
        }
    }

    Process();
}

THolder<TEvTxUserProxy::TEvProposeTransaction> TCreateUserSchemaActor::MakeMkDirRequest(const TString& root, const TString& dirName) {
    auto ev = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
    auto* trans = ev->Record.MutableTransaction()->MutableModifyScheme();

    trans->SetWorkingDir(root);
    trans->SetOperationType(NKikimrSchemeOp::ESchemeOpMkDir);
    trans->MutableMkDir()->SetName(dirName);
    RLOG_SQS_INFO("Making directory [" << dirName << "] as subdir of [" << root << "]");
    return ev;
}

void TCreateUserSchemaActor::Process() {
    switch (ECreating(SI_)) {
        case ECreating::MakeRootSqsDirectory: {
            CreateRootSqsDirAttemptWasMade_ = true;
            TStringBuf rootSqs = Root_;
            TStringBuf mainRoot, sqsDirName;
            rootSqs.RSplit('/', mainRoot, sqsDirName);
            if (mainRoot.empty() || sqsDirName.empty()) {
                RLOG_SQS_WARN("Failed to split root directory into components: [" << Root_ << "]");
                Send(Sender_, MakeHolder<TSqsEvents::TEvUserCreated>(false));
                PassAway();
                return;
            }
            Register(new TMiniKqlExecutionActor(SelfId(), RequestId_, MakeMkDirRequest(TString(mainRoot), TString(sqsDirName)), false, TQueuePath(Root_, UserName_, TString()), GetTransactionCounters(UserCounters_)));
            break;
        }
        case ECreating::MakeDirectory: {
            Register(new TMiniKqlExecutionActor(SelfId(), RequestId_, MakeMkDirRequest(Root_, UserName_), false, TQueuePath(Root_, UserName_, TString()), GetTransactionCounters(UserCounters_)));
            break;
        }
        case ECreating::Quoter: {
            Register(new TMiniKqlExecutionActor(
                SelfId(), RequestId_, MakeCreateKesusEvent(Root_ + "/" + UserName_, QUOTER_KESUS_NAME), false, TQueuePath(Root_, UserName_, TString()), GetTransactionCounters(UserCounters_))
            );
            break;
        }
        case ECreating::RPSQuota: {
            AddRPSQuota();
            break;
        }
        case ECreating::Finish: {
            Send(Sender_, MakeHolder<TSqsEvents::TEvUserCreated>(true));
            PassAway();
            break;
        }
    }
}

void TCreateUserSchemaActor::HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    const auto status = record.GetStatus();

    if (SuccessStatusCode(status)) {
        if (ECreating(SI_) == ECreating::Quoter) {
            KesusPathId_ = std::make_pair(record.GetSchemeShardTabletId(), record.GetPathId());
        }

        NextAction();
    } else {
        // Try to create /Root/SQS directory only if error occurs, because this is very rare operation.
        if (ECreating(SI_) == ECreating::MakeDirectory
            && record.GetSchemeShardStatus() == NKikimrScheme::EStatus::StatusPathDoesNotExist
            && !CreateRootSqsDirAttemptWasMade_)
        {
            RLOG_SQS_INFO("Root SQS directory does not exist, making it. Error record: " << record);
            SI_ = static_cast<int>(ECreating::MakeRootSqsDirectory);
            Process();
            // race with two creations of root sqs directory will result in success code on the second creation too.
            return;
        }
        RLOG_SQS_WARN("request execution error: " << record);

        Send(Sender_, MakeHolder<TSqsEvents::TEvUserCreated>(false));
        PassAway();
    }
}

void TCreateUserSchemaActor::HandleAddQuoterResource(NKesus::TEvKesus::TEvAddQuoterResourceResult::TPtr& ev) {
    AddQuoterResourceActor_ = TActorId();
    auto status = ev->Get()->Record.GetError().GetStatus();
    if (status == Ydb::StatusIds::SUCCESS || status == Ydb::StatusIds::ALREADY_EXISTS) {
        RLOG_SQS_DEBUG("Successfully added quoter resource. Id: " << ev->Get()->Record.GetResourceId());
        NextAction();
    } else {
        RLOG_SQS_WARN("Failed to add quoter resource: " << ev->Get()->Record);
        Send(Sender_, MakeHolder<TSqsEvents::TEvUserCreated>(false));
        PassAway();
    }
}

void TCreateUserSchemaActor::AddRPSQuota() {
    NKikimrKesus::TEvAddQuoterResource cmd;
    auto& res = *cmd.MutableResource();
    res.SetResourcePath(RPS_QUOTA_NAME);
    res.MutableHierarchicalDRRResourceConfig()->SetMaxUnitsPerSecond(1000);
    AddQuoterResourceActor_ = RunAddQuoterResource(KesusPathId_.first, KesusPathId_.second, cmd, RequestId_);
}

void TCreateUserSchemaActor::PassAway() {
    if (AddQuoterResourceActor_) {
        Send(AddQuoterResourceActor_, new TEvPoisonPill());
        AddQuoterResourceActor_ = TActorId();
    }
    TActorBootstrapped<TCreateUserSchemaActor>::PassAway();
}

TDeleteUserSchemaActor::TDeleteUserSchemaActor(const TString& root,
                                               const TString& name,
                                               const TActorId& sender,
                                               const TString& requestId,
                                               TIntrusivePtr<TUserCounters> userCounters)
    : Root_(root)
    , Name_(name)
    , Sender_(sender)
    , SI_(0)
    , RequestId_(requestId)
    , UserCounters_(std::move(userCounters))
{
}

TDeleteUserSchemaActor::~TDeleteUserSchemaActor() = default;

void TDeleteUserSchemaActor::Bootstrap() {
    Process();
    Become(&TThis::StateFunc);
}

void TDeleteUserSchemaActor::SkipQuoterStages() {
    if (EDeleting(SI_) == EDeleting::RemoveQuoter && (!Cfg().GetQuotingConfig().GetEnableQuoting() || !Cfg().GetQuotingConfig().HasKesusQuoterConfig())) {
        SI_++;
    }
}

void TDeleteUserSchemaActor::NextAction() {
    SI_++;

    Process();
}

void TDeleteUserSchemaActor::Process() {
    SkipQuoterStages();

    switch (EDeleting(SI_)) {
        case EDeleting::RemoveQuoter: {
            Register(new TMiniKqlExecutionActor(
                SelfId(), RequestId_, MakeRemoveKesusEvent(Root_ + "/" + Name_, QUOTER_KESUS_NAME), false, TQueuePath(Root_, Name_, TString()), GetTransactionCounters(UserCounters_))
            );
            break;
        }
        case EDeleting::RemoveDirectory: {
            Register(new TMiniKqlExecutionActor(
                SelfId(), RequestId_, MakeRemoveDirectoryEvent(Root_, Name_), false, TQueuePath(Root_, Name_, TString()), GetTransactionCounters(UserCounters_))
            );
            break;
        }
        case EDeleting::Finish: {
            Send(Sender_, MakeHolder<TSqsEvents::TEvUserDeleted>(true));
            PassAway();
            break;
        }
    }
}

void TDeleteUserSchemaActor::HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    const auto status = record.GetStatus();

    if (SuccessStatusCode(status)) {
        NextAction();
    } else {
        RLOG_SQS_WARN("request execution error: " << record);

        Send(Sender_, MakeHolder<TSqsEvents::TEvUserDeleted>(false, record.GetMiniKQLErrors()));
        PassAway();
    }
}

TAtomicCounterActor::TAtomicCounterActor(const TActorId& sender, const TString& rootPath, const TString& requestId)
    : Sender_(sender)
    , RootPath_(rootPath)
    , RequestId_(requestId)
{
}

TAtomicCounterActor::~TAtomicCounterActor() = default;

void TAtomicCounterActor::Bootstrap() {
    Become(&TThis::StateFunc);
    auto ev = MakeExecuteEvent(Sprintf(GetNextAtomicValueQuery, RootPath_.c_str()));
    Register(new TMiniKqlExecutionActor(SelfId(), RequestId_, std::move(ev), true, TQueuePath(), nullptr));
}

void TAtomicCounterActor::HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    const auto status = record.GetStatus();

    if (SuccessStatusCode(status)) {
        const TValue val(TValue::Create(record.GetExecutionEngineEvaluatedResponse()));
        Send(Sender_,
                 MakeHolder<TSqsEvents::TEvAtomicCounterIncrementResult>(true, "ok", val["value"]));
    } else {
        RLOG_SQS_ERROR("Failed to increment the atomic counter: bad status code");
        Send(Sender_,
                 MakeHolder<TSqsEvents::TEvAtomicCounterIncrementResult>(false));
    }
    PassAway();
}

template <class TEvCmd, class TEvCmdResult>
class TQuoterCmdRunner : public TActorBootstrapped<TQuoterCmdRunner<TEvCmd, TEvCmdResult>> {
public:
    TQuoterCmdRunner(
        ui64 quoterSchemeShardId,
        ui64 quoterPathId,
        const typename TEvCmd::ProtoRecordType& cmd,
        const TString& requestId,
        const TActorId& parent
    )
        : QuoterSchemeShardId(quoterSchemeShardId)
        , QuoterPathId(quoterPathId)
        , Cmd(cmd)
        , RequestId_(requestId)
        , Parent(parent)
    {
    }

    TQuoterCmdRunner(
        const TString& quoterPath,
        const typename TEvCmd::ProtoRecordType& cmd,
        const TString& requestId,
        const TActorId& parent
    )
        : QuoterPath(quoterPath)
        , Cmd(cmd)
        , RequestId_(requestId)
        , Parent(parent)
    {
    }

    void Bootstrap() {
        this->Become(&TQuoterCmdRunner::StateFunc);
        RequestQuoterTabletId();
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SQS_ACTOR;
    }

private:
    void RequestQuoterTabletId() {
        THolder<TEvTxUserProxy::TEvNavigate> request(new TEvTxUserProxy::TEvNavigate());
        auto& descCmd = *request->Record.MutableDescribePath();
        if (QuoterPath.empty()) {
            RLOG_SQS_TRACE("Requesting quoter tablet id for path id " << QuoterPathId);
            descCmd.SetSchemeshardId(QuoterSchemeShardId);
            descCmd.SetPathId(QuoterPathId);
        } else {
            RLOG_SQS_TRACE("Requesting quoter tablet id for path \"" << QuoterPath << "\"");
            descCmd.SetPath(QuoterPath);
        }
        this->Send(MakeTxProxyID(), std::move(request));
    }

    void HandleDescribeSchemeResult(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        RLOG_SQS_TRACE("HandleDescribeSchemeResult for quoter: " << ev->Get()->GetRecord());
        const auto& pathDescription = ev->Get()->GetRecord().GetPathDescription();
        if (ev->Get()->GetRecord().GetStatus() != NKikimrScheme::StatusSuccess || !pathDescription.GetKesus().GetKesusTabletId()) {
            RLOG_SQS_ERROR("Describe scheme failed: " << ev->Get()->GetRecord());
            SendErrorAndDie("Describe scheme failed.");
            return;
        }

        QuoterTabletId = pathDescription.GetKesus().GetKesusTabletId();
        CreatePipe(false);
        SendCmdToKesus();
    }

    void HandleCmdResult(typename TEvCmdResult::TPtr& ev) {
        RLOG_SQS_TRACE("Received answer from quoter: " << ev->Get()->Record);
        TActivationContext::Send(ev->Forward(Parent));
        PassAway();
    }

    void CreatePipe(bool retry) {
        NTabletPipe::TClientConfig clientConfig;
        clientConfig.RetryPolicy = {.RetryLimitCount = 5, .MinRetryTime = TDuration::MilliSeconds(100), .DoFirstRetryInstantly = !retry};
        PipeClient = this->Register(NTabletPipe::CreateClient(this->SelfId(), QuoterTabletId, clientConfig));
        RLOG_SQS_TRACE("Created pipe client to Kesus: " << PipeClient);
    }

    void SendCmdToKesus() {
        RLOG_SQS_DEBUG("Send command to Kesus: " << Cmd);
        NTabletPipe::SendData(this->SelfId(), PipeClient, new TEvCmd(Cmd));
    }

    void PassAway() override {
        if (PipeClient) {
            NTabletPipe::CloseClient(this->SelfId(), PipeClient);
            PipeClient = TActorId();
        }
        TActorBootstrapped<TQuoterCmdRunner>::PassAway();
    }

    void SendErrorAndDie(const TString& reason) {
        this->Send(Parent, MakeHolder<TEvCmdResult>(Ydb::StatusIds::INTERNAL_ERROR, reason));
        PassAway();
    }

    void HandlePipeClientConnected(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            RLOG_SQS_WARN("Failed to connect to pipe: " << ev->Get()->Status << ". Reconnecting");
            CreatePipe(true);
            SendCmdToKesus();
        }
    }

    void HandlePipeClientDisconnected(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        Y_UNUSED(ev);
        RLOG_SQS_WARN("Pipe disconnected. Reconnecting");
        CreatePipe(true);
        SendCmdToKesus();
    }

    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, HandleDescribeSchemeResult);
            hFunc(TEvCmdResult, HandleCmdResult);
            hFunc(TEvTabletPipe::TEvClientDestroyed, HandlePipeClientDisconnected);
            hFunc(TEvTabletPipe::TEvClientConnected, HandlePipeClientConnected);
            cFunc(TEvPoisonPill::EventType, PassAway);
        }
    }

private:
    const ui64 QuoterSchemeShardId = 0;
    const ui64 QuoterPathId = 0;
    const TString QuoterPath;
    const typename TEvCmd::ProtoRecordType Cmd;
    const TString RequestId_;
    const TActorId Parent;
    ui64 QuoterTabletId = 0;
    TActorId PipeClient;
};

TActorId RunAddQuoterResource(ui64 quoterSchemeShardId, ui64 quoterPathId, const NKikimrKesus::TEvAddQuoterResource& cmd, const TString& requestId) {
    return TActivationContext::Register(
        new TQuoterCmdRunner<NKesus::TEvKesus::TEvAddQuoterResource, NKesus::TEvKesus::TEvAddQuoterResourceResult>(
            quoterSchemeShardId, quoterPathId, cmd, requestId, TActivationContext::AsActorContext().SelfID
        )
    );
}

TActorId RunAddQuoterResource(const TString& quoterPath, const NKikimrKesus::TEvAddQuoterResource& cmd, const TString& requestId) {
    return TActivationContext::Register(
        new TQuoterCmdRunner<NKesus::TEvKesus::TEvAddQuoterResource, NKesus::TEvKesus::TEvAddQuoterResourceResult>(
            quoterPath, cmd, requestId, TActivationContext::AsActorContext().SelfID
        )
    );
}

TActorId RunDeleteQuoterResource(const TString& quoterPath, const NKikimrKesus::TEvDeleteQuoterResource& cmd, const TString& requestId) {
    return TActivationContext::Register(
        new TQuoterCmdRunner<NKesus::TEvKesus::TEvDeleteQuoterResource, NKesus::TEvKesus::TEvDeleteQuoterResourceResult>(
            quoterPath, cmd, requestId, TActivationContext::AsActorContext().SelfID
        )
    );
}

} // namespace NKikimr::NSQS
