#include <ydb/library/yql/providers/s3/events/events.h>

#include "yql_s3_actors_util.h"
#include "yql_s3_raw_read_actor.h"
#include "yql_s3_source_queue.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/yql/dq/actors/compute/retry_queue.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/providers/s3/common/util.h>

#include <library/cpp/string_utils/quote/quote.h>

#include <queue>

#define LOG_E(name, stream) \
    LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, name << ": " << this->SelfId() << ", TxId: " << TxId << ". " << stream)
#define LOG_W(name, stream) \
    LOG_WARN_S (*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, name << ": " << this->SelfId() << ", TxId: " << TxId << ". " << stream)
#define LOG_I(name, stream) \
    LOG_INFO_S (*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, name << ": " << this->SelfId() << ", TxId: " << TxId << ". " << stream)
#define LOG_D(name, stream) \
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, name << ": " << this->SelfId() << ", TxId: " << TxId << ". " << stream)
#define LOG_T(name, stream) \
    LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, name << ": " << this->SelfId() << ", TxId: " << TxId << ". " << stream)

namespace NYql::NDq {

ui64 SubtractSaturating(ui64 lhs, ui64 rhs) {
    return (lhs > rhs) ? lhs - rhs : 0;
}

class TS3ReadActor : public NActors::TActorBootstrapped<TS3ReadActor>, public IDqComputeActorAsyncInput {
public:
    TS3ReadActor(
        ui64 inputIndex,
        TCollectStatsLevel statsLevel,
        const TTxId& txId,
        IHTTPGateway::TPtr gateway,
        const NKikimr::NMiniKQL::THolderFactory& holderFactory,
        const TString& url,
        const TS3Credentials& credentials,
        const TString& pattern,
        NYql::NS3Lister::ES3PatternVariant patternVariant,
        NYql::NS3Details::TPathList&& paths,
        bool addPathIndex,
        const NActors::TActorId& computeActorId,
        ui64 sizeLimit,
        const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy,
        const TS3ReadActorFactoryConfig& readActorFactoryCfg,
        ::NMonitoring::TDynamicCounterPtr counters,
        ::NMonitoring::TDynamicCounterPtr taskCounters,
        ui64 fileSizeLimit,
        std::optional<ui64> rowsLimitHint,
        bool useRuntimeListing,
        NActors::TActorId fileQueueActor,
        ui64 fileQueueBatchSizeLimit,
        ui64 fileQueueBatchObjectCountLimit,
        ui64 fileQueueConsumersCountDelta)
        : ReadActorFactoryCfg(readActorFactoryCfg)
        , Gateway(std::move(gateway))
        , HolderFactory(holderFactory)
        , InputIndex(inputIndex)
        , TxId(txId)
        , ComputeActorId(computeActorId)
        , RetryPolicy(retryPolicy)
        , ActorSystem(NActors::TActivationContext::ActorSystem())
        , Url(url)
        , Credentials(credentials)
        , Pattern(pattern)
        , PatternVariant(patternVariant)
        , Paths(std::move(paths))
        , FileQueueActor(fileQueueActor)
        , AddPathIndex(addPathIndex)
        , SizeLimit(sizeLimit)
        , Counters(counters)
        , TaskCounters(taskCounters)
        , FileSizeLimit(fileSizeLimit)
        , FilesRemained(rowsLimitHint)
        , UseRuntimeListing(useRuntimeListing)
        , FileQueueBatchSizeLimit(fileQueueBatchSizeLimit)
        , FileQueueBatchObjectCountLimit(fileQueueBatchObjectCountLimit)
        , FileQueueConsumersCountDelta(fileQueueConsumersCountDelta) {
        if (Counters) {
            QueueDataSize = Counters->GetCounter("QueueDataSize");
            QueueDataLimit = Counters->GetCounter("QueueDataLimit");
            QueueBlockCount = Counters->GetCounter("QueueBlockCount");
            QueueDataLimit->Add(ReadActorFactoryCfg.DataInflight);
        }
        if (TaskCounters) {
            TaskQueueDataSize = TaskCounters->GetCounter("QueueDataSize");
            TaskQueueDataLimit = TaskCounters->GetCounter("QueueDataLimit");
            TaskQueueDataLimit->Add(ReadActorFactoryCfg.DataInflight);
        }
        IngressStats.Level = statsLevel;
    }

    void Bootstrap() {
        if (!UseRuntimeListing) {
            FileQueueActor = RegisterWithSameMailbox(CreateS3FileQueueActor(
                TxId,
                std::move(Paths),
                ReadActorFactoryCfg.MaxInflight * 2,
                FileSizeLimit,
                SizeLimit,
                false,
                1,
                FileQueueBatchSizeLimit,
                FileQueueBatchObjectCountLimit,
                Gateway,
                RetryPolicy,
                Url,
                Credentials,
                Pattern,
                PatternVariant,
                NYql::NS3Lister::ES3PatternType::Wildcard));
        }

        LOG_D("TS3ReadActor", "Bootstrap" << ", InputIndex: " << InputIndex << ", FileQueue: " << FileQueueActor << (UseRuntimeListing ? " (remote)" : " (local"));

        FileQueueEvents.Init(TxId, SelfId(), SelfId());
        FileQueueEvents.OnNewRecipientId(FileQueueActor);
        if (UseRuntimeListing && FileQueueConsumersCountDelta > 0) {
            FileQueueEvents.Send(new TEvS3Provider::TEvUpdateConsumersCount(FileQueueConsumersCountDelta));
        }
        SendPathBatchRequest();

        Become(&TS3ReadActor::StateFunc);
    }

    bool TryStartDownload() {
        TrySendPathBatchRequest();
        if (PathBatchQueue.empty()) {
            // no path is pending
            return false;
        }
        if (IsCurrentBatchEmpty) {
            // waiting for batch to finish
            return false;
        }
        if (QueueTotalDataSize > ReadActorFactoryCfg.DataInflight) {
            // too large data inflight
            return false;
        }
        if (DownloadInflight >= ReadActorFactoryCfg.MaxInflight) {
            // too large download inflight
            return false;
        }
        if (ConsumedEnoughFiles()) {
            // started enough downloads
            return false;
        }

        StartDownload();
        return true;
    }

    void StartDownload() {
        DownloadInflight++;
        const auto& object = ReadPathFromCache();
        auto url = Url + object.GetPath();
        auto id = object.GetPathIndex();
        const TString requestId = CreateGuidAsString();
        const auto& authInfo = Credentials.GetAuthInfo();
        LOG_D("TS3ReadActor", "Download: " << url << ", ID: " << id << ", request id: [" << requestId << "]");
        Gateway->Download(
            NS3Util::UrlEscapeRet(url),
            IHTTPGateway::MakeYcHeaders(requestId, authInfo.GetToken(), {}, authInfo.GetAwsUserPwd(), authInfo.GetAwsSigV4()),
            0U,
            std::min(object.GetSize(), SizeLimit),
            std::bind(&TS3ReadActor::OnDownloadFinished, ActorSystem, SelfId(), requestId, std::placeholders::_1, id, object.GetPath()),
            {},
            RetryPolicy);
    }

    NS3::FileQueue::TObjectPath ReadPathFromCache() {
        Y_ENSURE(!PathBatchQueue.empty());
        auto& currentBatch = PathBatchQueue.front();
        Y_ENSURE(!currentBatch.empty());
        auto object = currentBatch.back();
        currentBatch.pop_back();
        if (currentBatch.empty()) {
            PathBatchQueue.pop_front();
            IsCurrentBatchEmpty = true;
        }
        TrySendPathBatchRequest();
        return object;
    }
    void TrySendPathBatchRequest() {
        if (PathBatchQueue.size() < 2 && !IsFileQueueEmpty && !ConsumedEnoughFiles() && !IsWaitingFileQueueResponse) {
            SendPathBatchRequest();
        }
    }
    void SendPathBatchRequest() {
        FileQueueEvents.Send(new TEvS3Provider::TEvGetNextBatch());
        IsWaitingFileQueueResponse = true;
    }

    static constexpr char ActorName[] = "S3_READ_ACTOR";

private:
    void SaveState(const NDqProto::TCheckpoint&, NDqProto::TSourceState&) final {}
    void LoadState(const NDqProto::TSourceState&) final {}
    void CommitState(const NDqProto::TCheckpoint&) final {}

    ui64 GetInputIndex() const final {
        return InputIndex;
    }

    const TDqAsyncStats& GetIngressStats() const override {
        return IngressStats;
    }

    TDuration GetCpuTime() override {
        return CpuTime;
    }

    bool ConsumedEnoughFiles() const {
        return FilesRemained && (*FilesRemained == 0);
    }

    STRICT_STFUNC_EXC(StateFunc,
        hFunc(TEvS3Provider::TEvReadResult, Handle);
        hFunc(TEvS3Provider::TEvReadError, Handle);
        hFunc(TEvS3Provider::TEvObjectPathBatch, HandleObjectPathBatch);
        hFunc(TEvS3Provider::TEvObjectPathReadError, HandleObjectPathReadError);
        hFunc(TEvS3Provider::TEvAck, HandleAck);
        hFunc(NYql::NDq::TEvRetryQueuePrivate::TEvRetry, Handle);
        hFunc(NActors::TEvInterconnect::TEvNodeDisconnected, Handle);
        hFunc(NActors::TEvInterconnect::TEvNodeConnected, Handle);
        hFunc(NActors::TEvents::TEvUndelivered, Handle);
        , catch (const std::exception& e) {
            TIssues issues{TIssue{TStringBuilder() << "An unknown exception has occurred: '" << e.what() << "'"}};
            Send(ComputeActorId, new TEvAsyncInputError(InputIndex, issues, NYql::NDqProto::StatusIds::INTERNAL_ERROR));
        }
    )

    void HandleObjectPathBatch(TEvS3Provider::TEvObjectPathBatch::TPtr& objectPathBatch) {
        if (!FileQueueEvents.OnEventReceived(objectPathBatch)) {
            LOG_W("TS3ReadActor", "Duplicated TEvObjectPathBatch (likely resent) from " << FileQueueActor);
            return;
        }

        Y_ENSURE(IsWaitingFileQueueResponse);
        IsWaitingFileQueueResponse = false;
        auto& objectBatch = objectPathBatch->Get()->Record;
        ListedFiles += objectBatch.GetObjectPaths().size();
        IsFileQueueEmpty = objectBatch.GetNoMoreFiles();
        if (IsFileQueueEmpty && !IsConfirmedFileQueueFinish) {
            LOG_D("TS3ReadActor", "Confirm finish to " << FileQueueActor);
            SendPathBatchRequest();
            IsConfirmedFileQueueFinish = true;
        }
        if (!objectBatch.GetObjectPaths().empty()) {
            PathBatchQueue.emplace_back(
                std::make_move_iterator(objectBatch.MutableObjectPaths()->begin()),
                std::make_move_iterator(objectBatch.MutableObjectPaths()->end()));
        }
        while (TryStartDownload()) {}

        if (LastFileWasProcessed()) {
            Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
        }
    }
    void HandleObjectPathReadError(TEvS3Provider::TEvObjectPathReadError::TPtr& result) {
        if (!FileQueueEvents.OnEventReceived(result)) {
            LOG_W("TS3ReadActor", "Duplicated TEvObjectPathReadError (likely resent) from " << FileQueueActor);
            return;
        }

        IsFileQueueEmpty = true;
        if (!IsConfirmedFileQueueFinish) {
            LOG_D("TS3ReadActor", "Confirm finish (with errors) to " << FileQueueActor);
            SendPathBatchRequest();
            IsConfirmedFileQueueFinish = true;
        }
        TIssues issues;
        IssuesFromMessage(result->Get()->Record.GetIssues(), issues);
        LOG_E("TS3ReadActor", "Error while object listing, details: TEvObjectPathReadError: " << issues.ToOneLineString());
        issues = NS3Util::AddParentIssue(TStringBuilder{} << "Error while object listing", std::move(issues));
        Send(ComputeActorId, new TEvAsyncInputError(InputIndex, issues, NYql::NDqProto::StatusIds::EXTERNAL_ERROR));
    }

    void HandleAck(TEvS3Provider::TEvAck::TPtr& ev) {
        FileQueueEvents.OnEventReceived(ev);
    }
    
    static void OnDownloadFinished(NActors::TActorSystem* actorSystem, NActors::TActorId selfId, const TString& requestId, IHTTPGateway::TResult&& result, size_t pathInd, const TString path) {
        if (!result.Issues) {
            actorSystem->Send(new NActors::IEventHandle(selfId, NActors::TActorId(), new TEvS3Provider::TEvReadResult(std::move(result.Content), requestId, pathInd, path)));
        } else {
            actorSystem->Send(new NActors::IEventHandle(selfId, NActors::TActorId(), new TEvS3Provider::TEvReadError(std::move(result.Issues), requestId, pathInd, path)));
        }
    }

    i64 GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueBatch& buffer, TMaybe<TInstant>&, bool& finished, i64 freeSpace) final {
        i64 total = 0LL;
        if (!Blocks.empty()) {
            do {
                auto& content = std::get<IHTTPGateway::TContent>(Blocks.front());
                const auto size = content.size();
                auto value = NKikimr::NMiniKQL::MakeString(std::string_view(content));
                if (AddPathIndex) {
                    NUdf::TUnboxedValue* tupleItems = nullptr;
                    auto tuple = ContainerCache.NewArray(HolderFactory, 2, tupleItems);
                    *tupleItems++ = value;
                    *tupleItems++ = NUdf::TUnboxedValuePod(std::get<ui64>(Blocks.front()));
                    value = tuple;
                }

                buffer.emplace_back(std::move(value));
                Blocks.pop();
                total += size;
                freeSpace -= size;

                QueueTotalDataSize -= size;
                if (Counters) {
                    QueueDataSize->Sub(size);
                    QueueBlockCount->Dec();
                }
                if (TaskCounters) {
                    TaskQueueDataSize->Sub(size);
                }
                TryStartDownload();
            } while (!Blocks.empty() && freeSpace > 0LL);
        }

        if ((LastFileWasProcessed() || ConsumedEnoughFiles()) && !FileQueueEvents.RemoveConfirmedEvents()) {
            finished = true;
            ContainerCache.Clear();
        }

        if (!total) {
            IngressStats.TryPause();
        }

        return total;
    }
    bool LastFileWasProcessed() const {
        return Blocks.empty() && (ListedFiles == CompletedFiles) && IsFileQueueEmpty;
    }

    void Handle(TEvS3Provider::TEvReadResult::TPtr& result) {
        ++CompletedFiles;
        const auto id = result->Get()->PathIndex;
        const auto path = result->Get()->Path;
        const auto httpCode = result->Get()->Result.HttpResponseCode;
        const auto requestId = result->Get()->RequestId;
        LOG_D("TS3ReadActor", "ID: " << id << ", Path: " << path << ", read size: " << result->Get()->Result.size() << ", HTTP response code: " << httpCode << ", request id: [" << requestId << "]");
        if (200 == httpCode || 206 == httpCode) {
            auto size = result->Get()->Result.size();

            // in TS3ReadActor all files (aka Splits) are loaded in single Chunks
            IngressStats.Bytes += size;
            IngressStats.Rows++;
            IngressStats.Chunks++;
            IngressStats.Splits++;
            IngressStats.Resume();

            QueueTotalDataSize += size;
            if (Counters) {
                QueueBlockCount->Inc();
                QueueDataSize->Add(size);
            }
            if (TaskCounters) {
                TaskQueueDataSize->Add(size);
            }
            Blocks.emplace(std::make_tuple(std::move(result->Get()->Result), id));
            DownloadInflight--;
            if (IsCurrentBatchEmpty && DownloadInflight == 0) {
                IsCurrentBatchEmpty = false;
            }
            if (FilesRemained) {
                *FilesRemained = SubtractSaturating(*FilesRemained, 1);
            }
            TryStartDownload();
            Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
        } else {
            TString errorText = result->Get()->Result.Extract();
            TString errorCode;
            TString message;
            if (!ParseS3ErrorResponse(errorText, errorCode, message)) {
                message = errorText;
            }
            message = TStringBuilder{} << "Error while reading file " << path << ", details: " << message << ", request id: [" << requestId << "]";
            Send(ComputeActorId, new TEvAsyncInputError(InputIndex, BuildIssues(httpCode, errorCode, message), NYql::NDqProto::StatusIds::EXTERNAL_ERROR));
        }
    }

    void Handle(TEvS3Provider::TEvReadError::TPtr& result) {
        ++CompletedFiles;
        auto id = result->Get()->PathIndex;
        const auto requestId = result->Get()->RequestId;
        const auto path = result->Get()->Path;
        LOG_W("TS3ReadActor", "Error while reading file " << path << ", details: ID: " << id << ", TEvReadError: " << result->Get()->Error.ToOneLineString() << ", request id: [" << requestId << "]");
        auto issues = NS3Util::AddParentIssue(TStringBuilder{} << "Error while reading file " << path << " with request id [" << requestId << "]", TIssues{result->Get()->Error});
        Send(ComputeActorId, new TEvAsyncInputError(InputIndex, std::move(issues), NYql::NDqProto::StatusIds::EXTERNAL_ERROR));
    }
    
    void Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvRetry::TPtr&) {
        FileQueueEvents.Retry();
    }

    void Handle(NActors::TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        LOG_T("TS3ReadActor", "Handle disconnected FileQueue " << ev->Get()->NodeId);
        FileQueueEvents.HandleNodeDisconnected(ev->Get()->NodeId);
    }

    void Handle(NActors::TEvInterconnect::TEvNodeConnected::TPtr& ev) {
        LOG_T("TS3ReadActor", "Handle connected FileQueue " << ev->Get()->NodeId);
        FileQueueEvents.HandleNodeConnected(ev->Get()->NodeId);
    }

    void Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
        LOG_T("TS3ReadActor", "Handle undelivered FileQueue ");
        if (!FileQueueEvents.HandleUndelivered(ev)) {
            TIssues issues{TIssue{TStringBuilder() << "FileQueue was lost"}};
            Send(ComputeActorId, new TEvAsyncInputError(InputIndex, issues, NYql::NDqProto::StatusIds::UNAVAILABLE));
        }
    }

    // IActor & IDqComputeActorAsyncInput
    void PassAway() override { // Is called from Compute Actor
        LOG_D("TS3ReadActor", "PassAway");

        if (Counters) {
            QueueDataSize->Sub(QueueTotalDataSize);
            QueueBlockCount->Sub(Blocks.size());
            QueueDataLimit->Sub(ReadActorFactoryCfg.DataInflight);
        }
        if (TaskCounters) {
            TaskQueueDataSize->Sub(QueueTotalDataSize);
            TaskQueueDataLimit->Sub(ReadActorFactoryCfg.DataInflight);
        }
        QueueTotalDataSize = 0;

        ContainerCache.Clear();
        FileQueueEvents.Unsubscribe();
        TActorBootstrapped<TS3ReadActor>::PassAway();
    }

private:
    const TS3ReadActorFactoryConfig ReadActorFactoryCfg;
    const IHTTPGateway::TPtr Gateway;
    const NKikimr::NMiniKQL::THolderFactory& HolderFactory;
    NKikimr::NMiniKQL::TPlainContainerCache ContainerCache;

    const ui64 InputIndex;
    TDqAsyncStats IngressStats;
    const TTxId TxId;
    const NActors::TActorId ComputeActorId;
    const IHTTPGateway::TRetryPolicy::TPtr RetryPolicy;

    NActors::TActorSystem* const ActorSystem;

    const TString Url;
    const TS3Credentials Credentials;
    const TString Pattern;
    const NYql::NS3Lister::ES3PatternVariant PatternVariant;
    NYql::NS3Details::TPathList Paths;
    size_t ListedFiles = 0;
    size_t CompletedFiles = 0;
    NActors::TActorId FileQueueActor;
    const bool AddPathIndex;
    const ui64 SizeLimit;
    TDuration CpuTime;

    std::queue<std::tuple<IHTTPGateway::TContent, ui64>> Blocks;

    ::NMonitoring::TDynamicCounters::TCounterPtr QueueDataSize;
    ::NMonitoring::TDynamicCounters::TCounterPtr QueueDataLimit;
    ::NMonitoring::TDynamicCounters::TCounterPtr QueueBlockCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr DownloadPaused;
    ::NMonitoring::TDynamicCounters::TCounterPtr DeferredQueueSize;
    ::NMonitoring::TDynamicCounters::TCounterPtr TaskDownloadPaused;
    ::NMonitoring::TDynamicCounters::TCounterPtr TaskQueueDataSize;
    ::NMonitoring::TDynamicCounters::TCounterPtr TaskQueueDataLimit;
    ::NMonitoring::TDynamicCounterPtr Counters;
    ::NMonitoring::TDynamicCounterPtr TaskCounters;
    ui64 QueueTotalDataSize = 0;
    ui64 DownloadInflight = 0;
    const ui64 FileSizeLimit;
    std::optional<ui64> FilesRemained;

    bool UseRuntimeListing;
    ui64 FileQueueBatchSizeLimit;
    ui64 FileQueueBatchObjectCountLimit;
    ui64 FileQueueConsumersCountDelta;
    bool IsFileQueueEmpty = false;
    bool IsCurrentBatchEmpty = false;
    bool IsWaitingFileQueueResponse = false;
    bool IsConfirmedFileQueueFinish = false;
    TRetryEventsQueue FileQueueEvents;
    TDeque<TVector<NS3::FileQueue::TObjectPath>> PathBatchQueue;
};

std::pair<NYql::NDq::IDqComputeActorAsyncInput*, NActors::IActor*> CreateRawReadActor(
    ui64 inputIndex,
    TCollectStatsLevel statsLevel,
    const TTxId& txId,
    IHTTPGateway::TPtr gateway,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory,
    const TString& url,
    const TS3Credentials& credentials,
    const TString& pattern,
    NYql::NS3Lister::ES3PatternVariant patternVariant,
    NYql::NS3Details::TPathList&& paths,
    bool addPathIndex,
    const NActors::TActorId& computeActorId,
    ui64 sizeLimit,
    const IHTTPGateway::TRetryPolicy::TPtr& retryPolicy,
    const TS3ReadActorFactoryConfig& readActorFactoryCfg,
    ::NMonitoring::TDynamicCounterPtr counters,
    ::NMonitoring::TDynamicCounterPtr taskCounters,
    ui64 fileSizeLimit,
    std::optional<ui64> rowsLimitHint,
    bool useRuntimeListing,
    NActors::TActorId fileQueueActor,
    ui64 fileQueueBatchSizeLimit,
    ui64 fileQueueBatchObjectCountLimit,
    ui64 fileQueueConsumersCountDelta
) {
    const auto actor = new TS3ReadActor(
        inputIndex,
        statsLevel,
        txId,
        std::move(gateway),
        holderFactory,
        url,
        credentials,
        pattern,
        patternVariant,
        std::move(paths),
        addPathIndex,
        computeActorId,
        sizeLimit,
        retryPolicy,
        readActorFactoryCfg,
        counters,
        taskCounters,
        fileSizeLimit,
        rowsLimitHint,
        useRuntimeListing,
        fileQueueActor,
        fileQueueBatchSizeLimit,
        fileQueueBatchObjectCountLimit,
        fileQueueConsumersCountDelta
    );

    return {actor, actor};
}

} // namespace NYql::NDq
