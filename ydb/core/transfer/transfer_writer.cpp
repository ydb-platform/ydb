#include "events.h"
#include "logging.h"
#include "scheme.h"
#include "table_kind_state.h"
#include "transfer_writer.h"

#include <ydb/core/tx/replication/service/worker.h>

#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/core/fq/libs/row_dispatcher/purecalc_compilation/compile_service.h>
#include <ydb/core/tx/replication/ydb_proxy/topic_message.h>
#include "purecalc.h" // should be after topic_message
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/tx/scheme_cache/helpers.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/services/services.pb.h>

#include <yql/essentials/public/purecalc/helpers/stream/stream_from_vector.h>

using namespace NFq::NRowDispatcher;
using namespace NKikimr::NReplication::NService;

namespace NKikimr::NReplication::NTransfer {

namespace {


enum class ETag {
    FlushTimeout,
    RetryFlush,
    LeaveOnBatchSizeExceed
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

        if (entry.Status == TNavigate::EStatus::PathNotTable || (entry.Kind != TNavigate::KindColumnTable && entry.Kind != TNavigate::KindTable)) {
            return LogCritAndLeave("Only tables are supported as transfer targets");
        }

        if (!CheckEntrySucceeded(entry)) {
            return;
        }

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
        auto programHolder = CreateProgramHolder(TableState->GetScheme(), GenerateSql());
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

        auto r = dynamic_cast<IProgramHolder*>(ev->Get()->ProgramHolder.Release());
        Y_ENSURE(result, "Unexpected compile response");

        ProgramHolder = TIntrusivePtr<IProgramHolder>(r);

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

        if (!LastWriteTime) {
            LastWriteTime = TInstant::Now();
        }

        for (auto& message : records) {
            TMessage input {
                .PartitionId = partitionId,
                .Message = message
            };

            auto setError = [&](const auto& msg) {
                ProcessingErrorStatus = TEvWorker::TEvGone::EStatus::SCHEME_ERROR;
                ProcessingError = TStringBuilder() << "Error transform message partition " << partitionId << " offset " << message.GetOffset()
                    << ": " << msg;
            };

            try {
                auto result = ProgramHolder->GetProgram()->Apply(NYql::NPureCalc::StreamFromVector(TVector{input}));
                while (auto* m = result->Fetch()) {
                    if (ProcessingError || RequiredFlush) {
                        // We must get all the messages from the result, otherwise we will fall with an error inside yql
                        continue;
                    }

                    TString tablePath;
                    if (m->Table) {
                        if (TargetDirectoryPath) {
                            auto table = TFsPath(JoinPath({ DirectoryPath, m->Table.value()}));
                            if (table.IsSubpathOf(TargetDirectoryPath.value())) {
                                tablePath = table;
                            } else {
                                setError(TStringBuilder() << "the target table '" << m->Table.value() << "' is outside target directory");
                                continue;
                            }
                        } else {
                            setError("it is not allowed to specify a table to write");
                            continue;
                        }
                    } else {
                        tablePath = DefaultTablePath;
                    }

                    if (!TableState->AddData(std::move(tablePath), m->Data, m->EstimateSize)) {
                        RequiredFlush = true;
                    }
                }

                if (!ProcessingError && !RequiredFlush) {
                    LastProcessedOffset = message.GetOffset();
                }
            } catch (const yexception& e) {
                setError(e.what());
            }

            if (ProcessingError || RequiredFlush) {
                break;
            }
        }

        if (!ProcessingError && (TableState->BatchSize() >= BatchSizeBytes || *LastWriteTime < TInstant::Now() - FlushInterval || RequiredFlush)) {
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

        const auto status = ev->Get()->Status;
        const auto& error = ev->Get()->Issues.ToOneLineString();

        if (status != Ydb::StatusIds::SUCCESS && error && !ProcessingError) {
            ProcessingError = error;
        }

        if (ProcessingError) {
            return LogCritAndLeave(*ProcessingError);
        }

        if (LastProcessedOffset) {
            Send(Worker, new TEvWorker::TEvCommit(LastProcessedOffset.value() + 1));
        }

        if (RequiredFlush) {
            return Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup(ui32(ETag::LeaveOnBatchSizeExceed)));
        }

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
            case ETag::LeaveOnBatchSizeExceed:
                auto status = LastProcessedOffset ? TEvWorker::TEvGone::OVERLOAD : TEvWorker::TEvGone::SCHEME_ERROR;
                return Leave(status, "Overloaded: max batch size exceeded");
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

    void Retry() {
        Delay = Attempt++ ? Delay * 2 : MinRetryDelay;
        Delay = Min(Delay, MaxRetryDelay);
        const TDuration random = TDuration::FromValue(TAppData::RandomProvider->GenRand64() % Delay.MicroSeconds());
        this->Schedule(Delay + random, new TEvents::TEvWakeup(ui32(ETag::RetryFlush)));
    }

    void Leave(TEvWorker::TEvGone::EStatus status, const TString& message) {
        LOG_I("Leave: " << message);

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

    static std::optional<TFsPath> MakeTargetDirectoryPath(const TString& database, const TString& directoryPath) {
        if (directoryPath.empty()) {
            return std::nullopt;
        }

        TFsPath db(database);
        TFsPath dir(directoryPath);

        if (dir.IsNonStrictSubpathOf(db)) {
            return dir;
        }

        auto fullPath = dir.RelativePath(db);
        if (fullPath.IsNonStrictSubpathOf(db)) {
            return fullPath;
        }

        return std::nullopt;
    }

    explicit TTransferWriter(
            const TString& transformLambda,
            const TPathId& tablePathId,
            const TActorId& compileServiceId,
            const NKikimrReplication::TBatchingSettings& batchingSettings,
            const TString& directoryPath,
            const TString& database)
        : TransformLambda(transformLambda)
        , TablePathId(tablePathId)
        , CompileServiceId(compileServiceId)
        , FlushInterval(TDuration::MilliSeconds(std::max<ui64>(batchingSettings.GetFlushIntervalMilliSeconds(), 1000)))
        , BatchSizeBytes(std::min<ui64>(batchingSettings.GetBatchSizeBytes(), 1_GB))
        , DirectoryPath(directoryPath)
        , Database(database)
        , TargetDirectoryPath(MakeTargetDirectoryPath(database, directoryPath))
    {}

private:
    const TString TransformLambda;
    const TPathId TablePathId;
    const TActorId CompileServiceId;
    const TDuration FlushInterval;
    const ui64 BatchSizeBytes;
    const TString DirectoryPath;
    const TString Database;
    const std::optional<TFsPath> TargetDirectoryPath;
    TActorId Worker;

    TString DefaultTablePath;

    ITableKindState::TPtr TableState;

    size_t InFlightCompilationId = 0;
    IProgramHolder::TPtr ProgramHolder;

    mutable bool WakeupScheduled = false;
    mutable bool PollSent = false;
    mutable bool RequiredFlush = false;
    mutable std::optional<TInstant> LastWriteTime;

    mutable TMaybe<TString> LogPrefix;

    mutable TEvWorker::TEvGone::EStatus ProcessingErrorStatus;
    mutable TMaybe<TString> ProcessingError;

    ui32 PendingPartitionId = 0;
    std::optional<TVector<TTopicMessage>> PendingRecords;

    std::optional<size_t> LastProcessedOffset;

    ui32 Attempt = 0;
    TDuration Delay = MinRetryDelay;

}; // TTransferWriter

IActor* TTransferWriterFactory::Create(const Parameters& p) const {
    return new TTransferWriter(p.TransformLambda, p.TablePathId, p.CompileServiceId, p.BatchingSettings, p.DirectoryPath, p.Database);
}

}

