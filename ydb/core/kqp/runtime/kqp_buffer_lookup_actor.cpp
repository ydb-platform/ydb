#include "kqp_buffer_lookup_actor.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/kqp/common/kqp_locks_tli_helpers.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/runtime/kqp_read_iterator_common.h>
#include <ydb/core/kqp/runtime/kqp_stream_lookup_worker.h>
#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_log.h>
#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>
#include <yql/essentials/public/issue/yql_issue_message.h>


namespace NKikimr {
namespace NKqp {

namespace {

class TKqpBufferLookupActor : public NActors::TActorBootstrapped<TKqpBufferLookupActor>, public IKqpBufferTableLookup {
private:
    struct TEvPrivate {
        enum EEv {
            EvRetryRead = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
            EvSchemeCacheRequestTimeout
        };

        struct TEvSchemeCacheRequestTimeout : public TEventLocal<TEvSchemeCacheRequestTimeout, EvSchemeCacheRequestTimeout> {
        };

        struct TEvRetryRead : public TEventLocal<TEvRetryRead, EvRetryRead> {
            explicit TEvRetryRead(ui64 readId, ui64 lastSeqNo)
                : ReadId(readId)
                , LastSeqNo(lastSeqNo) {
            }

            const ui64 ReadId;
            const ui64 LastSeqNo;
        };
    };

    struct TLookupState {
        std::unique_ptr<TKqpStreamLookupWorker> Worker;
        ui64 ReadsInflight = 0;
        ui64 LookupColumnsCount = 0;

        bool IsAllReadsFinished() const {
            return ReadsInflight == 0;
        }
    };

    struct TShardState {
        bool HasPipe = false;
    };

    struct TReadState {
        const ui64 LookupCookie;
        const ui64 ShardId;
        ui64 LastSeqNo = 0;
        const bool IsUniqueCheck;
        const bool FailOnUniqueCheck;
        bool Blocked = false;

        ui64 RetryAttempts = 0;
    };

public:
    TKqpBufferLookupActor(TKqpBufferTableLookupSettings&& settings)
        : Settings(std::move(settings))
        , Partitioning(Settings.TxManager->GetPartitioning(Settings.TableId))
        , LogPrefix(TStringBuilder() << "Table: `" << Settings.TablePath << "` (" << Settings.TableId << "), "
            << "SessionActorId: " << Settings.SessionActorId)
        , LookupActorSpan(TWilsonKqp::LookupActor, std::move(Settings.ParentTraceId), "LookupActor") {
    }

    void Bootstrap() {
        CA_LOG_D("Start buffer lookup actor");

        Settings.Counters->StreamLookupActorsCount->Inc();
        Become(&TKqpBufferLookupActor::StateFunc);
    }

    static constexpr char ActorName[] = "KQP_BUFFER_LOOKUP_ACTOR";

    void PassAway() final {
        Settings.Counters->StreamLookupActorsCount->Dec();
        AFL_ENSURE(Settings.Alloc);
        {
            TGuard<NMiniKQL::TScopedAlloc> allocGuard(*Settings.Alloc);
            CookieToLookupState.clear();
        }

        for (const auto& [readId, state] : ReadIdToState) {
            Settings.Counters->SentIteratorCancels->Inc();
            auto cancel = MakeHolder<TEvDataShard::TEvReadCancel>();
            cancel->Record.SetReadId(readId);
            Send(PipeCacheId, new TEvPipeCache::TEvForward(cancel.Release(), state.ShardId, false));
        }

        Send(PipeCacheId, new TEvPipeCache::TEvUnlink(0));

        TActorBootstrapped<TKqpBufferLookupActor>::PassAway();

        LookupActorSpan.End();
    }

    void Terminate() override {
        PassAway();
    }

    STFUNC(StateFunc) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvDataShard::TEvReadResult, Handle);
                hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
                default:
                    RuntimeError(
                        NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                        NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
                        TStringBuilder() << "Unexpected event: " << ev->GetTypeRewrite());
            }
        } catch (const NKikimr::TMemoryLimitExceededException& e) {
            RuntimeError(
                NYql::NDqProto::StatusIds::PRECONDITION_FAILED,
                NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED,
                "Memory limit exceeded at stream lookup");
        } catch (const yexception& e) {
            RuntimeError(
                NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
                e.what());
        }
    }

    void SetLookupSettings(
            ui64 cookie,
            size_t lookupKeyPrefix,
            TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> keyColumns,
            TConstArrayRef<NKikimrKqp::TKqpColumnMetadataProto> lookupColumns) override {
        TLookupSettings settings {
            .TablePath = Settings.TablePath,
            .TableId = Settings.TableId,

            .AllowNullKeysPrefixSize = 0,
            .KeepRowsOrder = false,
            .LookupStrategy = NKqpProto::EStreamLookupStrategy::LOOKUP,

            .KeyColumns = {},
            .LookupKeyColumns = {},
            .Columns = {},
        };

        if (KeyColumnTypes.empty()) {
            for (const auto& keyColumn : keyColumns) {
                NScheme::TTypeInfo typeInfo = NScheme::TypeInfoFromProto(keyColumn.GetTypeId(), keyColumn.GetTypeInfo());
                KeyColumnTypes.push_back(typeInfo);
            }
        } else {
            AFL_ENSURE(KeyColumnTypes.size() == keyColumns.size());
        }

        {
            settings.KeyColumns.reserve(keyColumns.size());
            i32 keyOrder = 0;
            for (const auto& keyColumn : keyColumns) {
                NScheme::TTypeInfo typeInfo = NScheme::TypeInfoFromProto(keyColumn.GetTypeId(), keyColumn.GetTypeInfo());
                settings.KeyColumns.emplace(
                    keyColumn.GetName(),
                    TSysTables::TTableColumnInfo{
                        keyColumn.GetName(),
                        keyColumn.GetId(),
                        typeInfo,
                        keyColumn.GetTypeInfo().GetPgTypeMod(),
                        keyOrder++
                    }
                );
            }
        }

        {
            AFL_ENSURE(lookupKeyPrefix <= keyColumns.size());
            settings.LookupKeyColumns.reserve(lookupKeyPrefix);
            for (size_t index = 0; index < lookupKeyPrefix; ++index) {
                const auto& keyColumn = keyColumns.at(index);
                auto columnIt = settings.KeyColumns.find(keyColumn.GetName());
                YQL_ENSURE(columnIt != settings.KeyColumns.end());
                settings.LookupKeyColumns.push_back(&columnIt->second);
            }
        }

        {
            settings.Columns.reserve(keyColumns.size() + lookupColumns.size());
            for (size_t index = 0; index < keyColumns.size(); ++index) {
                const auto& keyColumn = keyColumns.at(index);
                NScheme::TTypeInfo typeInfo = NScheme::TypeInfoFromProto(keyColumn.GetTypeId(), keyColumn.GetTypeInfo());
                settings.Columns.push_back(
                    TSysTables::TTableColumnInfo{
                        keyColumn.GetName(),
                        keyColumn.GetId(),
                        typeInfo,
                        keyColumn.GetTypeInfo().GetPgTypeMod(),
                    }
                );
            }
            for (const auto& lookupColumn : lookupColumns) {
                NScheme::TTypeInfo typeInfo = NScheme::TypeInfoFromProto(lookupColumn.GetTypeId(), lookupColumn.GetTypeInfo());
                settings.Columns.push_back(
                    TSysTables::TTableColumnInfo{
                        lookupColumn.GetName(),
                        lookupColumn.GetId(),
                        typeInfo,
                        lookupColumn.GetTypeInfo().GetPgTypeMod(),
                    }
                );
            }
        }

        AFL_ENSURE(CookieToLookupState.emplace(
                cookie,
                TLookupState{
                    .Worker = CreateLookupWorker(std::move(settings), Settings.TypeEnv, Settings.HolderFactory),
                    .ReadsInflight = 0,
                    .LookupColumnsCount = lookupColumns.size(),
                }
            ).second);
    }

    void AddLookupTask(ui64 cookie, const std::vector<TConstArrayRef<TCell>>& keys) override {
        AddTask(cookie, keys, false, false);
    }

    void AddUniqueCheckTask(ui64 cookie, const std::vector<TConstArrayRef<TCell>>& keys, bool immediateFail) override {
        AddTask(cookie, keys, true, immediateFail);
    }

    void AddTask(ui64 cookie, const std::vector<TConstArrayRef<TCell>>& keys, bool isUnique, bool immediateFail) {
        auto& state = CookieToLookupState.at(cookie);
        auto& worker = state.Worker;

        AFL_ENSURE(state.ReadsInflight == 0);
        AFL_ENSURE(state.Worker->AllRowsProcessed());

        for (const auto& key : keys) {
            worker->AddInputRow(key);
        }

        StartLookupTask(cookie, state, isUnique, immediateFail);
    }

    void StartLookupTask(ui64 cookie, TLookupState& state, bool isUnique, bool uniqueFailOnRead) {
        auto& worker = state.Worker;
        auto reads = worker->BuildRequests(Partitioning, ReadId);

        // lookup can't be overloaded
        AFL_ENSURE(!worker->IsOverloaded(std::numeric_limits<size_t>::max()));

        for (auto& [shardId, read] : reads) {
            ++state.ReadsInflight;
            StartTableRead(cookie, shardId, isUnique, uniqueFailOnRead, std::move(read));
        }
    }

    bool HasResult(ui64 cookie) override {
        const auto& state = CookieToLookupState.at(cookie);
        return state.ReadsInflight == 0 && !state.Worker->AllRowsProcessed();
    }

    bool IsEmpty(ui64 cookie) override {
        const auto& state = CookieToLookupState.at(cookie);
        return state.ReadsInflight == 0 && state.Worker->AllRowsProcessed();
    }

    void ExtractResult(ui64 cookie, std::function<void(TConstArrayRef<TCell>)>&& callback) override {
        AFL_ENSURE(HasResult(cookie) || IsEmpty(cookie));
        auto& state = CookieToLookupState.at(cookie);
        auto& worker = state.Worker;

        const auto stats = worker->ReadAllResult(callback);
        ReadRowsCount += stats.ReadRowsCount;
        ReadBytesCount += stats.ReadBytesCount;
        AFL_ENSURE(worker->AllRowsProcessed());
    }

    TTableId GetTableId() const override {
        return Settings.TableId;
    }

    const TVector<NScheme::TTypeInfo>& GetKeyColumnTypes() const override {
        return KeyColumnTypes;
    }

    ui32 LookupColumnsCount(ui64 cookie) const override {
        return CookieToLookupState.at(cookie).LookupColumnsCount;
    }

    void StartTableRead(ui64 cookie, ui64 shardId, bool isUniqueCheck, bool failOnUniqueCheck, THolder<TEvDataShard::TEvRead> request) {
        Settings.Counters->CreatedIterators->Inc();
        auto& record = request->Record;

        Y_UNUSED(cookie);

        auto& worker = CookieToLookupState.at(cookie).Worker;

        CA_LOG_D("Start reading of table: " << worker->GetTablePath() << ", readId: " << record.GetReadId()
            << ", shardId: " << shardId);

        Settings.TxManager->AddShard(shardId, false, worker->GetTablePath());
        Settings.TxManager->AddAction(shardId, IKqpTransactionManager::EAction::READ);

        if (Settings.MvccSnapshot) {
            record.MutableSnapshot()->SetStep(Settings.MvccSnapshot->GetStep());
            record.MutableSnapshot()->SetTxId(Settings.MvccSnapshot->GetTxId());
        }

        AFL_ENSURE(Settings.LockTxId && Settings.LockNodeId);
        record.SetLockTxId(Settings.LockTxId);
        record.SetLockNodeId(Settings.LockNodeId);
        record.SetLockMode(!isUniqueCheck
            ? Settings.LockMode
            : NKikimrDataEvents::OPTIMISTIC);
        if (Settings.QuerySpanId) {
            record.SetQuerySpanId(Settings.QuerySpanId);
        }

        AFL_ENSURE(!failOnUniqueCheck || isUniqueCheck);

        if (failOnUniqueCheck) {
            record.SetTotalRowsLimit(1);
        }

        auto defaultSettings = GetDefaultReadSettings()->Record;
        record.SetMaxRows(defaultSettings.GetMaxRows());
        record.SetMaxBytes(defaultSettings.GetMaxBytes());
        record.SetResultFormat(NKikimrDataEvents::FORMAT_CELLVEC);

        CA_LOG_D(TStringBuilder() << "Send EvRead (buffer lookup) to shardId=" << shardId
            << ", readId = " << record.GetReadId()
            << ", tablePath: " << worker->GetTablePath()
            << ", snapshot=(txid=" << record.GetSnapshot().GetTxId() << ", step=" << record.GetSnapshot().GetStep() << ")"
            << ", lockTxId=" << record.GetLockTxId()
            << ", lockNodeId=" << record.GetLockNodeId());

        auto& shardState = ShardToState[shardId];

        const bool needToCreatePipe = !shardState.HasPipe;

        const auto readId = record.GetReadId();

        Send(PipeCacheId,
            new TEvPipeCache::TEvForward(
                request.Release(),
                shardId,
                TEvPipeCache::TEvForwardOptions{
                    .AutoConnect = needToCreatePipe,
                    .Subscribe = needToCreatePipe,
                }),
            IEventHandle::FlagTrackDelivery,
            0,
            LookupActorSpan.GetTraceId());

        shardState.HasPipe = true;

        AFL_ENSURE(ReadIdToState.emplace(
            readId,
            TReadState {
                .LookupCookie = cookie,
                .ShardId = shardId,
                .LastSeqNo = 0,
                .IsUniqueCheck = isUniqueCheck,
                .FailOnUniqueCheck = failOnUniqueCheck,
                .Blocked = false,
            }).second);
    }

    void Handle(TEvDataShard::TEvReadResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        auto readIt = ReadIdToState.find(record.GetReadId());
        if (readIt == ReadIdToState.end() || readIt->second.Blocked) {
            CA_LOG_D("Drop read with readId: " << record.GetReadId() << ", because it's already completed or blocked");
            return;
        }

        Settings.TxManager->AddParticipantNode(ev->Sender.NodeId());

        auto& read = readIt->second;
        const auto shardId = read.ShardId;
        const auto cookie = read.LookupCookie;

        auto& shardState = ShardToState.at(shardId);

        auto& lookupState = CookieToLookupState.at(cookie);
        AFL_ENSURE(lookupState.Worker);
        AFL_ENSURE(lookupState.ReadsInflight > 0);

        CA_LOG_D("Recv TEvReadResult (buffer lookup) from ShardID=" << shardId
            << ", Table = " << lookupState.Worker->GetTablePath()
            << ", ReadId=" << record.GetReadId() << " (current ReadId=" << ReadId << ")"
            << ", SeqNo=" << record.GetSeqNo()
            << ", Status=" << Ydb::StatusIds::StatusCode_Name(record.GetStatus().GetCode())
            << ", Finished=" << record.GetFinished()
            << ", RowCount=" << record.GetRowCount()
            << ", TxLocks= " << [&]() {
                TStringBuilder builder;
                for (const auto& lock : record.GetTxLocks()) {
                    builder << lock.ShortDebugString();
                }
                return builder;
            }()
            << ", BrokenTxLocks= " << [&]() {
                TStringBuilder builder;
                for (const auto& lock : record.GetBrokenTxLocks()) {
                    builder << lock.ShortDebugString();
                }
                return builder;
            }());

        if (!record.GetBrokenTxLocks().empty()) {
            BrokenLocksCount += record.GetBrokenTxLocks().size();
            Settings.TxManager->SetError(shardId);
            if (record.HasDeferredVictimQuerySpanId()) {
                Settings.TxManager->SetVictimQuerySpanId(record.GetDeferredVictimQuerySpanId());
            } else {
                SetVictimQuerySpanIdFromBrokenLocks(shardId, record.GetBrokenTxLocks(), Settings.TxManager);
            }
            RuntimeError(NYql::NDqProto::StatusIds::ABORTED,
                NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED,
                MakeLockInvalidatedMessage(Settings.TxManager, lookupState.Worker->GetTablePath()));
            return;
        }

        for (const auto& lock : record.GetTxLocks()) {
            if (!Settings.TxManager->AddLock(shardId, lock, Settings.QuerySpanId)) {
                YQL_ENSURE(Settings.TxManager->BrokenLocks());
                RuntimeError(NYql::NDqProto::StatusIds::ABORTED,
                    NYql::TIssuesIds::KIKIMR_LOCKS_INVALIDATED,
                    MakeLockInvalidatedMessage(Settings.TxManager, lookupState.Worker->GetTablePath()));
                return;
            }
        }

        auto getIssues = [&record]() {
            NYql::TIssues issues;
            NYql::IssuesFromMessage(record.GetStatus().GetIssues(), issues);
            return issues;
        };

        switch (record.GetStatus().GetCode()) {
            case Ydb::StatusIds::SUCCESS:
                break;
            case Ydb::StatusIds::NOT_FOUND:
            {
                return RuntimeError(
                    NYql::NDqProto::StatusIds::UNAVAILABLE,
                    NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
                    TStringBuilder() << "Table: `"
                        << lookupState.Worker->GetTablePath() << "` not found.",
                    getIssues());
            }
            case Ydb::StatusIds::OVERLOADED: {
                CA_LOG_D("OVERLOADED was received from tablet: " << shardId << "."
                    << getIssues().ToOneLineString());
                if (!RetryTableRead(record.GetReadId(), false)) {
                    return RuntimeError(
                        NYql::NDqProto::StatusIds::OVERLOADED,
                        NYql::TIssuesIds::KIKIMR_OVERLOADED,
                        TStringBuilder() << "Table: `"
                            << lookupState.Worker->GetTablePath() << "` retry limit exceeded.",
                        getIssues());
                }
                return;
            }
            case Ydb::StatusIds::INTERNAL_ERROR: {
                CA_LOG_D("INTERNAL_ERROR was received from tablet: " << shardId << "."
                    << getIssues().ToOneLineString());
                if (!RetryTableRead(record.GetReadId(), true)) {
                    return RuntimeError(
                        NYql::NDqProto::StatusIds::INTERNAL_ERROR,
                        NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
                        TStringBuilder() << "Table: `"
                            << lookupState.Worker->GetTablePath() << "` retry limit exceeded.",
                        getIssues());
                }
                return;
            }
            default: {
                return RuntimeError(
                        NYql::NDqProto::StatusIds::ABORTED,
                        NYql::TIssuesIds::KIKIMR_OPERATION_ABORTED,
                        "Read request aborted",
                        getIssues());
            }
        }

        Settings.Counters->DataShardIteratorMessages->Inc();
        if (record.GetStatus().GetCode() != Ydb::StatusIds::SUCCESS) {
            Settings.Counters->DataShardIteratorFails->Inc();
        }

        AFL_ENSURE(read.LastSeqNo < record.GetSeqNo());
        read.LastSeqNo = record.GetSeqNo();

        // Save values before potentially erasing the read state
        const bool failOnUniqueCheck = read.FailOnUniqueCheck;
        const bool finished = record.GetFinished();

        if (finished) {
            --lookupState.ReadsInflight;
            ReadIdToState.erase(readIt);
        } else {
            AFL_ENSURE(record.HasContinuationToken());
            NKikimrTxDataShard::TReadContinuationToken continuationToken;
            AFL_ENSURE(continuationToken.ParseFromString(record.GetContinuationToken()));
            AFL_ENSURE(!continuationToken.HasLastProcessedKey()); // can't read more than 1 row per range

            Settings.Counters->SentIteratorAcks->Inc();
            THolder<TEvDataShard::TEvReadAck> request(new TEvDataShard::TEvReadAck());
            request->Record.SetReadId(record.GetReadId());
            request->Record.SetSeqNo(record.GetSeqNo());

            auto defaultSettings = GetDefaultReadAckSettings()->Record;
            request->Record.SetMaxRows(defaultSettings.GetMaxRows());
            request->Record.SetMaxBytes(defaultSettings.GetMaxBytes());

            const bool needToCreatePipe = !shardState.HasPipe;

            Send(PipeCacheId,
                new TEvPipeCache::TEvForward(
                    request.Release(), shardId, TEvPipeCache::TEvForwardOptions{
                        .AutoConnect = needToCreatePipe,
                        .Subscribe = needToCreatePipe,
                    }),
                IEventHandle::FlagTrackDelivery,
                0,
                LookupActorSpan.GetTraceId());

            shardState.HasPipe = true;
            CA_LOG_D("TEvReadAck was sent to shard: " << shardId);
        }

        if (failOnUniqueCheck && record.GetRowCount() != 0) {
            return RuntimeError(
                    NYql::NDqProto::StatusIds::PRECONDITION_FAILED,
                    NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED,
                    "Conflict with existing key.");
        }

        {
            const auto guard = Settings.TypeEnv.BindAllocator();
            lookupState.Worker->AddResult(TKqpStreamLookupWorker::TShardReadResult{
                shardId, THolder<TEventHandle<TEvDataShard::TEvReadResult>>(ev.Release())
            });
        }

        Settings.Callbacks->OnLookupTaskFinished();
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        CA_LOG_D("TEvDeliveryProblem was received from tablet: " << ev->Get()->TabletId);
        ShardToState.at(ev->Get()->TabletId).HasPipe = false;

        TVector<ui64> toRetry;
        for (const auto& [readId, readState] : ReadIdToState) {
            if (readState.ShardId == ev->Get()->TabletId && !readState.Blocked) {
                Settings.Counters->IteratorDeliveryProblems->Inc();
                toRetry.push_back(readId);
            }
        }

        for (const auto& readId : toRetry) {
            if (!RetryTableRead(readId, true)) {
                const auto& failedRead = ReadIdToState.at(readId);
                const auto& lookupState = CookieToLookupState.at(failedRead.LookupCookie);
                return RuntimeError(
                    NYql::NDqProto::StatusIds::UNAVAILABLE,
                    NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
                    TStringBuilder() << "Table: `"
                        << lookupState.Worker->GetTablePath() << "` retry limit exceeded.",
                    {});
            }
        }
    }

    void Handle(TEvPrivate::TEvRetryRead::TPtr& ev) {
        const ui64 failedReadId = ev->Get()->ReadId;
        auto readIt = ReadIdToState.find(failedReadId);
        if (readIt == ReadIdToState.end()) {
            CA_LOG_D("received retry request for already finished/non-existing read, read_id: " << failedReadId);
            return;
        }

        auto& failedRead = readIt->second;
        auto& lookupState = CookieToLookupState.at(failedRead.LookupCookie);
        YQL_ENSURE(!failedRead.Blocked || failedRead.LastSeqNo == ev->Get()->LastSeqNo);
        if (failedRead.LastSeqNo <= ev->Get()->LastSeqNo) {
            DoRetryTableRead(failedReadId, lookupState, failedRead);
        }
    }

    bool RetryTableRead(const ui64 failedReadId, bool allowInstantRetry) {
        auto& failedRead = ReadIdToState.at(failedReadId);
        auto& lookupState = CookieToLookupState.at(failedRead.LookupCookie);
        CA_LOG_D("Retry reading of table: " << lookupState.Worker->GetTablePath() << ", failedReadId: " << failedReadId
            << ", shardId: " << failedRead.ShardId);
        failedRead.Blocked = true;

        if (failedRead.RetryAttempts >= MaxShardRetries()) {
            return false;
        }

        auto delay = CalcDelay(failedRead.RetryAttempts, allowInstantRetry);
        if (delay == TDuration::Zero()) {
            DoRetryTableRead(failedReadId, lookupState, failedRead);
        } else {
            TlsActivationContext->Schedule(
                delay, new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvRetryRead(failedReadId, failedRead.LastSeqNo))
            );
        }

        return true;
    }

    void DoRetryTableRead(const ui64 failedReadId, TLookupState& lookupState, TReadState& failedRead) {
        AFL_ENSURE(failedRead.Blocked);
        --lookupState.ReadsInflight;
        const auto guard = Settings.TypeEnv.BindAllocator();
        auto requests = lookupState.Worker->RebuildRequest(failedReadId, ReadId);
        for (auto& request : requests) {
            const ui64 newReadId = request->Record.GetReadId();
            ++lookupState.ReadsInflight;
            StartTableRead(failedRead.LookupCookie, failedRead.ShardId, failedRead.IsUniqueCheck, failedRead.FailOnUniqueCheck, std::move(request));
            ReadIdToState.at(newReadId).RetryAttempts = failedRead.RetryAttempts + 1;
        }
        ReadIdToState.erase(failedReadId);
    }

    void RuntimeError(
            NYql::NDqProto::StatusIds::StatusCode statusCode,
            NYql::EYqlIssueCode id,
            const TString& message,
            const NYql::TIssues& subIssues = {}) {
        if (LookupActorSpan) {
            LookupActorSpan.EndError(message);
        }
        Settings.Callbacks->OnLookupError(statusCode, id, message, subIssues);
    }

    void FillStats(NYql::NDqProto::TDqTaskStats* stats) override {
        NYql::NDqProto::TDqTableStats* tableStats = nullptr;
        for (size_t i = 0; i < stats->TablesSize(); ++i) {
            auto* table = stats->MutableTables(i);
            if (table->GetTablePath() == Settings.TablePath) {
                tableStats = table;
            }
        }
        if (!tableStats) {
            tableStats = stats->AddTables();
            tableStats->SetTablePath(Settings.TablePath);
        }

        tableStats->SetReadRows(tableStats->GetReadRows() + ReadRowsCount);
        tableStats->SetReadBytes(tableStats->GetReadBytes() + ReadBytesCount);

        ReadRowsCount = 0;
        ReadBytesCount = 0;

        // Add lock stats for broken locks
        if (BrokenLocksCount > 0) {
            NKqpProto::TKqpTaskExtraStats extraStats;
            if (stats->HasExtra()) {
                stats->GetExtra().UnpackTo(&extraStats);
            }
            extraStats.MutableLockStats()->SetBrokenAsVictim(
                extraStats.GetLockStats().GetBrokenAsVictim() + BrokenLocksCount);
            stats->MutableExtra()->PackFrom(extraStats);
            BrokenLocksCount = 0;
        }
    }

private:
    TKqpBufferTableLookupSettings Settings;
    std::shared_ptr<const TVector<TKeyDesc::TPartitionInfo>> Partitioning;
    const TString LogPrefix;
    TVector<NScheme::TTypeInfo> KeyColumnTypes;

    const TActorId PipeCacheId = NKikimr::MakePipePerNodeCacheID(false);

    THashMap<ui64, TLookupState> CookieToLookupState;
    THashMap<ui64, TShardState> ShardToState;
    THashMap<ui64, TReadState> ReadIdToState;

    ui64 ReadId = 0;

    // stats
    ui64 ReadRowsCount = 0;
    ui64 ReadBytesCount = 0;
    ui64 BrokenLocksCount = 0;

    NWilson::TSpan LookupActorSpan;
};

}

std::pair<IKqpBufferTableLookup*, NActors::IActor*> CreateKqpBufferTableLookup(TKqpBufferTableLookupSettings&& settings) {
    auto* ptr = new TKqpBufferLookupActor(std::move(settings));
    return {ptr, ptr};
}

}
}
