#include "events.h"
#include "logging.h"
#include "scheme.h"
#include "table_kind_state.h"
#include "transfer_writer.h"

#include <ydb/core/tx/replication/service/worker.h>

#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/core/fq/libs/row_dispatcher/purecalc_compilation/compile_service.h>
#include <ydb/core/kqp/runtime/kqp_write_table.h>
#include <ydb/core/tx/replication/ydb_proxy/topic_message.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>
#include "purecalc.h" // should be after topic_message
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/tx/scheme_cache/helpers.h>
#include <ydb/core/tx/tx_proxy/upload_rows_common_impl.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <yql/essentials/providers/common/schema/parser/yql_type_parser.h>
#include <yql/essentials/public/purecalc/helpers/stream/stream_from_vector.h>

using namespace NFq::NRowDispatcher;
using namespace NKikimr::NReplication::NService;

namespace NKikimr::NReplication::NTransfer {

namespace {

using namespace NYql::NPureCalc;
using namespace NKikimr::NMiniKQL;

struct TOutputType {
    std::optional<TString> Table;
    NUdf::TUnboxedValue Value;
    NMiniKQL::TUnboxedValueBatch Data;
};

class TMessageOutputSpec : public NYql::NPureCalc::TOutputSpecBase {
public:
    explicit TMessageOutputSpec(const TScheme& tableScheme, const NYT::TNode& schema)
        : TableScheme(tableScheme)
        , Schema(schema)
    {}

public:
    const NYT::TNode& GetSchema() const override {
        return Schema;
    }

    const TVector<NKikimrKqp::TKqpColumnMetadataProto>& GetTableColumns() const {
        return TableScheme.ColumnsMetadata;
    }

    const TVector<NKikimrKqp::TKqpColumnMetadataProto>& GetStructColumns() const {
        return TableScheme.StructMetadata;
    }

private:
    const TScheme TableScheme;
    const NYT::TNode Schema;
};

class TOutputListImpl final: public IStream<TOutputType*> {
protected:
    TWorkerHolder<IPullListWorker> WorkerHolder_;
    const TMessageOutputSpec& OutputSpec;

public:
    explicit TOutputListImpl(const TMessageOutputSpec& outputSpec, TWorkerHolder<IPullListWorker> worker)
        : WorkerHolder_(std::move(worker))
        , OutputSpec(outputSpec)
    {
        Row.resize(1);
    }

public:
    TOutputType* Fetch() override {
        TBindTerminator bind(WorkerHolder_->GetGraph().GetTerminator());

        with_lock(WorkerHolder_->GetScopedAlloc()) {
            Out.Data.clear();

            NYql::NUdf::TUnboxedValue value;

            if (!WorkerHolder_->GetOutputIterator().Next(value)) {
                return nullptr;
            }

            Out.Value = value.GetElement(0);

            const auto& columns = OutputSpec.GetStructColumns();
            for (size_t i = 0; i < columns.size(); ++i) {
                const auto& column = columns[i];
                if (column.name() == SystemColumns::TargetTable) {
                    auto e = Out.Value.GetElement(i);
                    if (e) {
                        auto opt = e.GetOptionalValue();
                        if (opt) {
                            Out.Table = opt.AsStringRef();
                        }
                    }
                    continue;
                }

                if (column.GetNotNull() && !Out.Value.GetElement(i)) {
                    throw yexception() << "The value of the '" << column.GetName() << "' column must be non-NULL";
                }
            }

            Out.Data.PushRow(&Out.Value, 1);

            return &Out;
        }
    }

private:
    std::vector<NUdf::TUnboxedValue> Row;
    TOutputType Out;
};

} // namespace

} // namespace NKikimr::NReplication::NTransfer


template <>
struct NYql::NPureCalc::TOutputSpecTraits<NKikimr::NReplication::NTransfer::TMessageOutputSpec> {
    static const constexpr bool IsPartial = false;

    static const constexpr bool SupportPullListMode = true;

    using TOutputItemType = NKikimr::NReplication::NTransfer::TOutputType*;
    using TPullStreamReturnType = THolder<IStream<TOutputItemType>>;
    using TPullListReturnType = THolder<IStream<TOutputItemType>>;

    static TPullListReturnType ConvertPullListWorkerToOutputType(
        const NKikimr::NReplication::NTransfer::TMessageOutputSpec& outputSpec,
        TWorkerHolder<IPullListWorker> worker
    ) {
        return MakeHolder<NKikimr::NReplication::NTransfer::TOutputListImpl>(outputSpec, std::move(worker));
    }
};


namespace NKikimr::NReplication::NTransfer {

namespace {


class TProgramHolder : public NFq::IProgramHolder {
public:
    using TPtr = TIntrusivePtr<TProgramHolder>;

public:
    TProgramHolder(
        const TScheme& tableScheme,
        const TString& sql
    )
        : TopicColumns()
        , TableScheme(tableScheme)
        , Sql(sql)
    {}

public:
    void CreateProgram(NYql::NPureCalc::IProgramFactoryPtr programFactory) override {
        // Program should be stateless because input values
        // allocated on another allocator and should be released
        Program = programFactory->MakePullListProgram(
            NYdb::NTopic::NPurecalc::TMessageInputSpec(),
            TMessageOutputSpec(TableScheme, MakeOutputSchema(TableScheme.TableColumns)),
            Sql,
            NYql::NPureCalc::ETranslationMode::SQL
        );
    }

    NYql::NPureCalc::TPullListProgram<NYdb::NTopic::NPurecalc::TMessageInputSpec, TMessageOutputSpec>* GetProgram() {
        return Program.Get();
    }

private:
    const TVector<TSchemeColumn> TopicColumns;
    const TScheme TableScheme;
    const TString Sql;

    THolder<NYql::NPureCalc::TPullListProgram<NYdb::NTopic::NPurecalc::TMessageInputSpec, TMessageOutputSpec>> Program;
};



enum class ETag {
    FlushTimeout,
    RetryFlush
};

} // anonymous namespace

class TTransferWriter
    : public TActorBootstrapped<TTransferWriter>
    , private NSchemeCache::TSchemeCacheHelpers
{
    static constexpr TDuration MinRetryDelay = TDuration::Seconds(1);
    static constexpr TDuration MaxRetryDelay = TDuration::Minutes(10);

public:
    void Bootstrap() {
        GetTableScheme();
    }

private:
    void GetTableScheme() {
        LOG_D("GetTableScheme: worker# " << Worker);
        Become(&TThis::StateGetTableScheme);

        auto request = MakeHolder<TNavigate>();
        request->ResultSet.emplace_back(MakeNavigateEntry(TablePathId, TNavigate::OpTable));
        Send(MakeSchemeCacheID(), new TEvNavigate(request.Release()));
    }

    STFUNC(StateGetTableScheme) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);

            hFunc(TEvWorker::TEvHandshake, Handle);
            hFunc(TEvWorker::TEvData, HoldHandle);
            sFunc(TEvents::TEvWakeup, GetTableScheme);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

    void LogCritAndLeave(const TString& error) {
        LOG_C(error);
        Leave(TEvWorker::TEvGone::SCHEME_ERROR, error);
    }

    void LogWarnAndRetry(const TString& error) {
        LOG_W(error);
        Retry();
    }

    template <typename CheckFunc, typename FailFunc, typename T, typename... Args>
    bool Check(CheckFunc checkFunc, FailFunc failFunc, const T& subject, Args&&... args) {
        return checkFunc("writer", subject, std::forward<Args>(args)..., std::bind(failFunc, this, std::placeholders::_1));
    }

    template <typename T>
    bool CheckNotEmpty(const TAutoPtr<T>& result) {
        return Check(&TSchemeCacheHelpers::CheckNotEmpty<T>, &TThis::LogCritAndLeave, result);
    }

    template <typename T>
    bool CheckEntriesCount(const TAutoPtr<T>& result, ui32 expected) {
        return Check(&TSchemeCacheHelpers::CheckEntriesCount<T>, &TThis::LogCritAndLeave, result, expected);
    }

    template <typename T>
    bool CheckTableId(const T& entry, const TTableId& expected) {
        return Check(&TSchemeCacheHelpers::CheckTableId<T>, &TThis::LogCritAndLeave, entry, expected);
    }

    template <typename T>
    bool CheckEntrySucceeded(const T& entry) {
        return Check(&TSchemeCacheHelpers::CheckEntrySucceeded<T>, &TThis::LogWarnAndRetry, entry);
    }

    template <typename T>
    bool CheckEntryKind(const T& entry, TNavigate::EKind expected) {
        return Check(&TSchemeCacheHelpers::CheckEntryKind<T>, &TThis::LogCritAndLeave, entry, expected);
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        auto& result = ev->Get()->Request;

        LOG_D("Handle TEvTxProxySchemeCache::TEvNavigateKeySetResult"
            << ": result# " << (result ? result->ToString(*AppData()->TypeRegistry) : "nullptr"));

        if (!CheckNotEmpty(result)) {
            return;
        }

        if (!CheckEntriesCount(result, 1)) {
            return;
        }

        const auto& entry = result->ResultSet.at(0);

        if (!CheckTableId(entry, TablePathId)) {
            return;
        }

        // TODO support row tables
        if (entry.Status == TNavigate::EStatus::PathNotTable || (entry.Kind != TNavigate::KindColumnTable && entry.Kind != TNavigate::KindTable)) {
            return LogCritAndLeave("Only tables are supported as transfer targets");
        }

        if (!CheckEntrySucceeded(entry)) {
            return;
        }

        auto path = entry.Path;
        path.pop_back();
        Database = JoinPath(path);

        DefaultTablePath = JoinPath(entry.Path);

        if (entry.Kind == TNavigate::KindColumnTable) {
            TableState = CreateColumnTableState(SelfId(), result);
        } else {
            TableState = CreateRowTableState(SelfId(), result);
        }

        CompileTransferLambda();
    }

private:
    void CompileTransferLambda() {
        LOG_D("CompileTransferLambda: worker# " << Worker);

        NFq::TPurecalcCompileSettings settings = {};
        auto programHolder = MakeIntrusive<TProgramHolder>(TableState->GetScheme(), GenerateSql());
        auto result = std::make_unique<NFq::TEvRowDispatcher::TEvPurecalcCompileRequest>(std::move(programHolder), settings);

        Send(CompileServiceId, result.release(), 0, ++InFlightCompilationId);
        Become(&TThis::StateCompileTransferLambda);
    }

    STFUNC(StateCompileTransferLambda) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NFq::TEvRowDispatcher::TEvPurecalcCompileResponse, Handle);

            hFunc(TEvWorker::TEvHandshake, Handle);
            hFunc(TEvWorker::TEvData, HoldHandle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

    TString GenerateSql() {
        TStringBuilder sb;
        sb << TransformLambda;
        sb << "SELECT * FROM (\n";
        sb << "  SELECT $__ydb_transfer_lambda(TableRow()) AS " << SystemColumns::Root << " FROM Input\n";
        sb << ") FLATTEN BY " << SystemColumns::Root << ";\n";
        LOG_T("SQL: " << sb);
        return sb;
    }

    void Handle(NFq::TEvRowDispatcher::TEvPurecalcCompileResponse::TPtr& ev) {
        const auto& result = ev->Get();

        LOG_D("Handle TEvPurecalcCompileResponse"
            << ": result# " << (result ? result->Issues.ToOneLineString() : "nullptr"));

        if (ev->Cookie != InFlightCompilationId) {
            LOG_D("Outdated compiler response ignored for id " << ev->Cookie << ", current compile id " << InFlightCompilationId);
            return;
        }

        if (!result->ProgramHolder) {
            return LogCritAndLeave(TStringBuilder() << "Compilation failed: " << result->Issues.ToOneLineString());
        }

        auto r = dynamic_cast<TProgramHolder*>(ev->Get()->ProgramHolder.Release());
        Y_ENSURE(result, "Unexpected compile response");

        ProgramHolder = TIntrusivePtr<TProgramHolder>(r);

        StartWork();
    }

private:
    void StartWork() {
        Become(&TThis::StateWork);

        Attempt = 0;
        Delay = MinRetryDelay;

        if (PendingRecords) {
            ProcessData(PendingPartitionId, *PendingRecords);
            PendingRecords.reset();
        }

        if (!WakeupScheduled) {
            WakeupScheduled = true;
            Schedule(FlushInterval, new TEvents::TEvWakeup(ui32(ETag::FlushTimeout)));
        }
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWorker::TEvHandshake, Handle);
            hFunc(TEvWorker::TEvData, Handle);

            sFunc(TEvents::TEvPoison, PassAway);
            sFunc(TEvents::TEvWakeup, TryFlush);
        }
    }

    void Handle(TEvWorker::TEvHandshake::TPtr& ev) {
        Worker = ev->Sender;
        LOG_D("Handshake"
            << ": worker# " << Worker);

        if (ProcessingError) {
            Leave(ProcessingErrorStatus, *ProcessingError);
        } else {
            PollSent = true;
            Send(Worker, new TEvWorker::TEvHandshake());
        }
    }

    void HoldHandle(TEvWorker::TEvData::TPtr& ev) {
        Y_ABORT_UNLESS(!PendingRecords);
        PendingPartitionId = ev->Get()->PartitionId;
        PendingRecords = std::move(ev->Get()->Records);
    }

    void Handle(TEvWorker::TEvData::TPtr& ev) {
        LOG_D("Handle TEvData record count: " << ev->Get()->Records.size());
        ProcessData(ev->Get()->PartitionId, ev->Get()->Records);
    }

    void ProcessData(const ui32 partitionId, const TVector<TTopicMessage>& records) {
        if (!records) {
            Send(Worker, new TEvWorker::TEvGone(TEvWorker::TEvGone::DONE));
            return;
        }

        PollSent = false;

        //TableState->EnshureDataBatch();
        if (!LastWriteTime) {
            LastWriteTime = TInstant::Now();
        }

        for (auto& message : records) {
            NYdb::NTopic::NPurecalc::TMessage input;
            input.Data = std::move(message.GetData());
            input.MessageGroupId = std::move(message.GetMessageGroupId());
            input.Partition = partitionId;
            input.ProducerId = std::move(message.GetProducerId());
            input.Offset = message.GetOffset();
            input.SeqNo = message.GetSeqNo();

            try {
                auto result = ProgramHolder->GetProgram()->Apply(NYql::NPureCalc::StreamFromVector(TVector{input}));
                while (auto* m = result->Fetch()) {
                    TString tablePath;
                    if (m->Table) {
                        tablePath = JoinPath({ Database, m->Table.value()});
                    } else {
                        tablePath = DefaultTablePath;
                    }
                    
                    Cerr << ">>>>> TABLE = " << tablePath << Endl;
                    TableState->AddData(std::move(tablePath), m->Data);
                }

                LastProcessedOffset = message.GetOffset();
            } catch (const yexception& e) {
                ProcessingErrorStatus = TEvWorker::TEvGone::EStatus::SCHEME_ERROR;
                ProcessingError = TStringBuilder() << "Error transform message partition " << partitionId << " offset " << message.GetOffset() << ": " << e.what();
                break;
            }
        }

        if (!ProcessingError && (TableState->BatchSize() >= BatchSizeBytes || *LastWriteTime < TInstant::Now() - FlushInterval)) {
            if (TableState->Flush()) {
                LastWriteTime.reset();
                return Become(&TThis::StateWrite);
            }
        }

        if (ProcessingError) {
            LogCritAndLeave(*ProcessingError);
        } else {
            PollSent = true;
            Send(Worker, new TEvWorker::TEvPoll(true));
        }
    }

    void TryFlush() {
        if (LastWriteTime && LastWriteTime < TInstant::Now() - FlushInterval && TableState->Flush()) {
            LastWriteTime.reset();
            WakeupScheduled = false;
            Become(&TThis::StateWrite);
        } else {
            Schedule(FlushInterval, new TEvents::TEvWakeup(ui32(ETag::FlushTimeout)));
        }
    }

private:
    STFUNC(StateWrite) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvCompleted, Handle);
            hFunc(NTransferPrivate::TEvWriteCompleeted, Handle);

            hFunc(TEvWorker::TEvHandshake, Handle);
            hFunc(TEvWorker::TEvData, HoldHandle);

            sFunc(TEvents::TEvPoison, PassAway);
            hFunc(TEvents::TEvWakeup, WriteWakeup);
        }
    }

    void Handle(NTransferPrivate::TEvWriteCompleeted::TPtr& ev) {
        LOG_D("Handle NTransferPrivate::TEvWriteCompleeted"
            << ": worker# " << Worker
            << " status# " << ev->Get()->Status
            << " issues# " << ev->Get()->Issues.ToOneLineString());

        HandleWriteResult(ev->Get()->Status, ev->Get()->Issues.ToOneLineString());
    }

    void Handle(TEvents::TEvCompleted::TPtr& ev) {
        LOG_D("Handle TEvents::TEvCompleted"
            << ": worker# " << Worker
            << " status# " << ev->Get()->Status);

        //auto [error, _] = TableState->Handle(ev);
        //HandleWriteResult(ev->Get()->Status, error);
    }

    void HandleWriteResult(ui32 status, const TString& error) {
        if (status != Ydb::StatusIds::SUCCESS && error && !ProcessingError) {
            ProcessingError = error;
        }

        if (ProcessingError) {
            return LogCritAndLeave(*ProcessingError);
        }

        Send(Worker, new TEvWorker::TEvCommit(LastProcessedOffset + 1));
        if (!PollSent) {
            PollSent = true;
            Send(Worker, new TEvWorker::TEvPoll());
        }

        if (LastWriteTime) {
            LastWriteTime = TInstant::Now();
        }

        return StartWork();
    }

    void WriteWakeup(TEvents::TEvWakeup::TPtr& ev) {
        switch(ETag(ev->Get()->Tag)) {
            case ETag::FlushTimeout:
                WakeupScheduled = false;
                break;
            case ETag::RetryFlush:
                TableState->Flush();
                break;
        }
    }

private:

    bool PendingLeave() {
        return PendingRecords && PendingRecords->empty();
    }

    TStringBuf GetLogPrefix() const {
        if (!LogPrefix) {
            LogPrefix = TStringBuilder()
                << "[TransferWriter]"
                << SelfId() << " ";
        }

        return LogPrefix.GetRef();
    }

    template <typename TResult>
    bool CheckResult(const TResult& result, const TStringBuf marker) {
        if (result.IsSuccess()) {
            return true;
        }

        LOG_E("Error at '" << marker << "'"
            << ", error# " << result);
        RetryOrLeave(result.GetError());

        return false;
    }

    void Retry() {
        Delay = Attempt++ ? Delay * 2 : MinRetryDelay;
        Delay = Min(Delay, MaxRetryDelay);
        const TDuration random = TDuration::FromValue(TAppData::RandomProvider->GenRand64() % Delay.MicroSeconds());
        this->Schedule(Delay + random, new TEvents::TEvWakeup(ui32(ETag::RetryFlush)));
    }

    void Leave(TEvWorker::TEvGone::EStatus status, const TString& message) {
        LOG_I("Leave");

        if (Worker) {
            Send(Worker, new TEvWorker::TEvGone(status, message));
            PassAway();
        } else {
            ProcessingErrorStatus = status;
            ProcessingError = message;
        }
    }

    void PassAway() override {
        TActor::PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::REPLICATION_TRANSFER_WRITER;
    }

    explicit TTransferWriter(
            const TString& transformLambda,
            const TPathId& tablePathId,
            const TActorId& compileServiceId,
            const NKikimrReplication::TBatchingSettings& batchingSettings)
        : TransformLambda(transformLambda)
        , TablePathId(tablePathId)
        , CompileServiceId(compileServiceId)
        , FlushInterval(TDuration::MilliSeconds(std::max<ui64>(batchingSettings.GetFlushIntervalMilliSeconds(), 1000)))
        , BatchSizeBytes(std::min<ui64>(batchingSettings.GetBatchSizeBytes(), 1_GB))
    {}

private:
    const TString TransformLambda;
    const TPathId TablePathId;
    const TActorId CompileServiceId;
    const TDuration FlushInterval;
    const ui64 BatchSizeBytes;
    TActorId Worker;

    TString Database;
    TString DefaultTablePath;

    ITableKindState::TPtr TableState;

    size_t InFlightCompilationId = 0;
    TProgramHolder::TPtr ProgramHolder;

    mutable bool WakeupScheduled = false;
    mutable bool PollSent = false;
    mutable std::optional<TInstant> LastWriteTime;

    mutable TMaybe<TString> LogPrefix;

    mutable TEvWorker::TEvGone::EStatus ProcessingErrorStatus;
    mutable TMaybe<TString> ProcessingError;

    ui32 PendingPartitionId = 0;
    std::optional<TVector<TTopicMessage>> PendingRecords;

    size_t LastProcessedOffset = 0;

    ui32 Attempt = 0;
    TDuration Delay = MinRetryDelay;

}; // TTransferWriter

IActor* TTransferWriterFactory::Create(const Parameters& p) const {
    return new TTransferWriter(p.TransformLambda, p.TablePathId, p.CompileServiceId, p.BatchingSettings);
}

}

