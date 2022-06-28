#include "datastreams_proxy.h"
#include "put_records_actor.h"
#include "shard_iterator.h"
#include "next_token.h"

#include <ydb/core/grpc_services/service_datastreams.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_deferrable.h>
#include <ydb/core/grpc_services/rpc_scheme_base.h>
#include <ydb/core/persqueue/partition.h>
#include <ydb/core/persqueue/write_meta.h>

#include <ydb/services/lib/actors/pq_schema_actor.h>
#include <ydb/services/lib/sharding/sharding.h>
#include <ydb/services/persqueue_v1/actors/persqueue_utils.h>

#include <util/folder/path.h>

#include <iterator>

using namespace NActors;
using namespace NKikimrClient;

using grpc::Status;


namespace NKikimr::NDataStreams::V1 {
    const TString YDS_SERVICE_TYPE = "data-streams";
    const i32 DEFAULT_STREAM_DAY_RETENTION = TDuration::Days(1).Hours();
    // const i32 DEFAULT_STREAM_WEEK_RETENTION = TDuration::Days(7).Hours();

    using namespace NGRpcService;
    using namespace NGRpcProxy::V1;


    namespace {

        template <class TRequest>
        const TRequest* GetRequest(NGRpcService::IRequestOpCtx *request)
        {
            return dynamic_cast<const TRequest*>(request->GetRequest());
        }

        ui32 PartitionWriteSpeedInBytesPerSec(ui32 speedInKbPerSec) {
            return speedInKbPerSec == 0 ? 1_MB : speedInKbPerSec * 1_KB;
        }

        template <typename T>
        static constexpr bool has_storage_megabytes(...) { return false; }
        template <typename T>
        static constexpr bool has_storage_megabytes(bool, decltype((std::declval<T>().retention_storage_megabytes()))* = 0) { return true; }
        template<class TRequest>
        bool ValidateRetentionLimits(const TRequest& req,
                                     const NKikimrSchemeOp::TPersQueueGroupDescription& groupConfig,
                                     TMaybe<bool> increase, TString& errorText) {
            if constexpr (has_storage_megabytes<TRequest>(true)) {
                if (req.has_retention_storage_megabytes()) {
                    if (req.retention_storage_megabytes() * 1_MB < 50_GB) {
                        errorText = TStringBuilder() << "Retention space should be greater than 50GB: "
                                                     << req.retention_storage_megabytes();
                        return false;
                    } else {
                        return true;
                    }
                }
                if (!req.has_retention_period_hours() &&
                    !req.has_retention_storage_megabytes()) {
                    return true;
                }
            }
            if (req.retention_period_hours() <= 0 ||
                req.retention_period_hours() > DEFAULT_STREAM_DAY_RETENTION) {
                errorText = TStringBuilder() << "Retention period should be non negative and less than " <<
                    DEFAULT_STREAM_DAY_RETENTION << " hours: " << req.retention_period_hours();
                return false;
            }

            if (increase.Defined()) {
                auto currentLifetime = TDuration::Seconds(groupConfig.GetPQTabletConfig().GetPartitionConfig().GetLifetimeSeconds());
                auto newLifetime = TDuration::Hours(req.retention_period_hours());
                if (*increase) {
                    if (newLifetime <= currentLifetime) {
                        errorText = TStringBuilder() << "Retention period is not greater than provided: "
                                                     << currentLifetime.Hours() << " <= " << newLifetime.Hours();
                        return false;
                    }
                } else {
                    if (newLifetime >= currentLifetime) {
                        errorText = TStringBuilder() << "Retention period is not less than provided: "
                                                     << currentLifetime.Hours() << " <= " << newLifetime.Hours();
                        return false;
                    }
                }
            }

            return true;
        }

        template<class TRequest>
        bool ValidateShardsCount(const TRequest& req, const NKikimrSchemeOp::TPersQueueGroupDescription& groupConfig, TString& errorText) {
            if (req.target_shard_count() < (i32)groupConfig.GetTotalGroupCount()) {
                errorText = TStringBuilder() << "Shard count must be non-decreasing, old value is "
                                             << groupConfig.GetTotalGroupCount()
                                             << " new value is " << req.target_shard_count();
                return false;
            }
            return true;
        }

        template<class TRequest>
        bool ValidateWriteSpeedLimit(const TRequest& req, TString& errorText, const TActorContext& ctx) {
            THashSet<ui32> validLimits {0};
            if (AppData(ctx)->PQConfig.ValidWriteSpeedLimitsKbPerSecSize() == 0) {
                validLimits.insert(128);
                validLimits.insert(1_KB);
            } else {
                const auto& limits = AppData(ctx)->PQConfig.GetValidWriteSpeedLimitsKbPerSec();
                validLimits.insert(limits.begin(), limits.end());
            }
            if (validLimits.find(req.write_quota_kb_per_sec()) == validLimits.end()) {
                errorText = TStringBuilder() << "write_quota_kb_per_sec must have values from set {" << JoinSeq(",", validLimits) << ", got " << req.write_quota_kb_per_sec();
                return false;
            }

            return true;
        }

    }

    class TCreateStreamActor : public TPQGrpcSchemaBase<TCreateStreamActor, NKikimr::NGRpcService::TEvDataStreamsCreateStreamRequest> {
        using TBase = TPQGrpcSchemaBase<TCreateStreamActor, TEvDataStreamsCreateStreamRequest>;
        using TProtoRequest = typename TBase::TProtoRequest;

    public:
        TCreateStreamActor(NKikimr::NGRpcService::IRequestOpCtx* request);
        ~TCreateStreamActor() = default;

        void Bootstrap(const NActors::TActorContext& ctx);
        void FillProposeRequest(TEvTxUserProxy::TEvProposeTransaction& proposal, const TActorContext& ctx,
                                const TString& workingDir, const TString& name);
        void StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx);
        void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx);
        void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev, const TActorContext& ctx);
    };


    TCreateStreamActor::TCreateStreamActor(NKikimr::NGRpcService::IRequestOpCtx* request)
        : TBase(request, GetRequest<TProtoRequest>(request)->stream_name())
    {
    }

    void TCreateStreamActor::Bootstrap(const NActors::TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        SendProposeRequest(ctx);
        Become(&TCreateStreamActor::StateWork);
    }

    void TCreateStreamActor::HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ev);
        Y_UNUSED(ctx);
    }


    void TCreateStreamActor::FillProposeRequest(TEvTxUserProxy::TEvProposeTransaction& proposal,
            const TActorContext& ctx, const TString& workingDir, const TString& name)
    {
        NKikimrSchemeOp::TModifyScheme& modifyScheme(*proposal.Record.MutableTransaction()->MutableModifyScheme());

        Ydb::PersQueue::V1::TopicSettings topicSettings;
        topicSettings.set_partitions_count(GetProtoRequest()->shard_count());
        switch (GetProtoRequest()->retention_case()) {
            case Ydb::DataStreams::V1::CreateStreamRequest::RetentionCase::kRetentionPeriodHours:
                topicSettings.set_retention_period_ms(
                    TDuration::Hours(GetProtoRequest()->retention_period_hours()).MilliSeconds());
                break;
            case Ydb::DataStreams::V1::CreateStreamRequest::RetentionCase::kRetentionStorageMegabytes:
                topicSettings.set_retention_storage_bytes(
                    GetProtoRequest()->retention_storage_megabytes() * 1_MB);
                break;
            default:
                topicSettings.set_retention_period_ms(
                    TDuration::Hours(DEFAULT_STREAM_DAY_RETENTION).MilliSeconds());
        }
        topicSettings.set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
        topicSettings.add_supported_codecs(Ydb::PersQueue::V1::CODEC_RAW);
        topicSettings.set_max_partition_write_speed(
            PartitionWriteSpeedInBytesPerSec(GetProtoRequest()->write_quota_kb_per_sec()));
        topicSettings.set_max_partition_write_burst(
            PartitionWriteSpeedInBytesPerSec(GetProtoRequest()->write_quota_kb_per_sec()));

        if (workingDir != proposal.Record.GetDatabaseName() && !proposal.Record.GetDatabaseName().empty()) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::BAD_REQUEST,
                                  "streams can be created only at database root", ctx);
        }

        TString error;
        auto status = NKikimr::NGRpcProxy::V1::FillProposeRequestImpl(name, topicSettings, modifyScheme, ctx, false, error,
                                                                      workingDir, proposal.Record.GetDatabaseName());
        modifyScheme.SetWorkingDir(workingDir);

        if (!error.Empty()) {
            return ReplyWithError(status, Ydb::PersQueue::ErrorCode::BAD_REQUEST, error, ctx);
        }

        auto pqDescr = modifyScheme.MutableCreatePersQueueGroup();
        pqDescr->SetPartitionPerTablet(1);
        if (GetProtoRequest()->retention_case() ==
            Ydb::DataStreams::V1::CreateStreamRequest::RetentionCase::kRetentionStorageMegabytes) {
            modifyScheme.MutableCreatePersQueueGroup()->MutablePQTabletConfig()->
                MutablePartitionConfig()->SetLifetimeSeconds(TDuration::Days(7).Seconds());
        }

        if (!ValidateWriteSpeedLimit(*GetProtoRequest(), error, ctx) ||
            !ValidateRetentionLimits(*GetProtoRequest(), *pqDescr, Nothing(), error)) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::BAD_REQUEST,
                                  error, ctx);
        }
    }

    void TCreateStreamActor::Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev, const TActorContext& ctx) {
        auto msg = ev->Get();
        const auto status = static_cast<TEvTxUserProxy::TEvProposeTransactionStatus::EStatus>(ev->Get()->Record.GetStatus());
        if (status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete
            && msg->Record.GetSchemeShardStatus() == NKikimrScheme::EStatus::StatusAlreadyExists)
        {
            return ReplyWithError(Ydb::StatusIds::ALREADY_EXISTS,
                                  Ydb::PersQueue::ErrorCode::ERROR,
                                  TStringBuilder() << "Stream with name " << GetProtoRequest()->stream_name() << " is already exists",
                                  ctx);
        }
        return TBase::TBase::Handle(ev, ctx);
    }

    void TCreateStreamActor::StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxUserProxy::TEvProposeTransactionStatus, Handle);
            default: TBase::StateWork(ev, ctx);
        }
    }

    //-----------------------------------------------------------------------------------

    class TDeleteStreamActor : public TPQGrpcSchemaBase<TDeleteStreamActor, NKikimr::NGRpcService::TEvDataStreamsDeleteStreamRequest> {
        using TBase = TPQGrpcSchemaBase<TDeleteStreamActor, TEvDataStreamsDeleteStreamRequest>;
        using TProtoRequest = typename TBase::TProtoRequest;

    public:
        TDeleteStreamActor(NKikimr::NGRpcService::IRequestOpCtx* request);
        ~TDeleteStreamActor() = default;

        void Bootstrap(const NActors::TActorContext& ctx);

        void FillProposeRequest(TEvTxUserProxy::TEvProposeTransaction& proposal, const TActorContext& ctx,
                                const TString& workingDir, const TString& name);

        void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev,
                                         const TActorContext& ctx);

    private:
        bool EnforceDeletion;
    };

    TDeleteStreamActor::TDeleteStreamActor(NKikimr::NGRpcService::IRequestOpCtx* request)
        : TBase(request, GetRequest<TProtoRequest>(request)->stream_name())
        , EnforceDeletion{GetProtoRequest()->enforce_consumer_deletion()}
    {
    }

    void TDeleteStreamActor::Bootstrap(const NActors::TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        SendDescribeProposeRequest(ctx);
        Become(&TDeleteStreamActor::StateWork);
    }

    void TDeleteStreamActor::FillProposeRequest(TEvTxUserProxy::TEvProposeTransaction& proposal,
                                                const TActorContext& ctx, const TString& workingDir,
                                                const TString& name)
    {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, "WorkingDir = " << workingDir << ", name = " << name);
        NKikimrSchemeOp::TModifyScheme& modifyScheme(*proposal.Record.MutableTransaction()->MutableModifyScheme());
        modifyScheme.SetWorkingDir(workingDir);
        modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpDropPersQueueGroup);
        modifyScheme.MutableDrop()->SetName(name);
    }

    void TDeleteStreamActor::HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev,
                                                         const TActorContext& ctx) {
        if (ReplyIfNotTopic(ev, ctx)) {
            return;
        }

        const auto& response = ev->Get()->Request.Get()->ResultSet.front();
        const auto& pqGroupDescription = response.PQGroupInfo->Description;
        const auto& readRules = pqGroupDescription.GetPQTabletConfig().GetReadRules();

        if (readRules.size() > 0 && EnforceDeletion == false) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::ERROR,
                                  TStringBuilder() << "Stream has registered consumers" <<
                                  "and EnforceConsumerDeletion flag is false", ctx);
        }

        SendProposeRequest(ctx);
    }
    //-----------------------------------------------------------------------------------------------------------

    class TUpdateShardCountActor : public TUpdateSchemeActor<TUpdateShardCountActor, TEvDataStreamsUpdateShardCountRequest> {
        using TBase = TUpdateSchemeActor<TUpdateShardCountActor, TEvDataStreamsUpdateShardCountRequest>;
        using TProtoRequest = typename TBase::TProtoRequest;
    public:
        TUpdateShardCountActor(NKikimr::NGRpcService::IRequestOpCtx* request)
            : TBase(request, GetRequest<TProtoRequest>(request)->stream_name())
        {
        }

        void Bootstrap(const TActorContext& ctx);
        void ModifyPersqueueConfig(const TActorContext& ctx,
                                   NKikimrSchemeOp::TPersQueueGroupDescription& groupConfig,
                                   const NKikimrSchemeOp::TPersQueueGroupDescription& pqGroupDescription,
                                   const NKikimrSchemeOp::TDirEntry& selfInfo);
    };

    void TUpdateShardCountActor::Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        SendDescribeProposeRequest(ctx);
        Become(&TBase::StateWork);
    }

    void TUpdateShardCountActor::ModifyPersqueueConfig(
        const TActorContext& ctx,
        NKikimrSchemeOp::TPersQueueGroupDescription& groupConfig,
        const NKikimrSchemeOp::TPersQueueGroupDescription& pqGroupDescription,
        const NKikimrSchemeOp::TDirEntry& selfInfo
    ) {
        Y_UNUSED(selfInfo);

        TString error;
        if (!ValidateShardsCount(*GetProtoRequest(), pqGroupDescription, error)) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::BAD_REQUEST, error, ctx);
        }

        groupConfig.SetTotalGroupCount(GetProtoRequest()->target_shard_count());
    }

    //-----------------------------------------------------------------------------------------------------------

    class TUpdateStreamActor : public TUpdateSchemeActor<TUpdateStreamActor, TEvDataStreamsUpdateStreamRequest> {
        using TBase = TUpdateSchemeActor<TUpdateStreamActor, TEvDataStreamsUpdateStreamRequest>;
        using TProtoRequest = typename TBase::TProtoRequest;

    public:
        TUpdateStreamActor(NKikimr::NGRpcService::IRequestOpCtx* request)
                : TBase(request, GetRequest<TProtoRequest>(request)->stream_name())
        {
        }

        void Bootstrap(const TActorContext& ctx);
        void ModifyPersqueueConfig(const TActorContext& ctx,
                                   NKikimrSchemeOp::TPersQueueGroupDescription& groupConfig,
                                   const NKikimrSchemeOp::TPersQueueGroupDescription& pqGroupDescription,
                                   const NKikimrSchemeOp::TDirEntry& selfInfo);
    };

    void TUpdateStreamActor::Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        SendDescribeProposeRequest(ctx);
        Become(&TBase::StateWork);
    }

    void TUpdateStreamActor::ModifyPersqueueConfig(
        const TActorContext& ctx,
        NKikimrSchemeOp::TPersQueueGroupDescription& groupConfig,
        const NKikimrSchemeOp::TPersQueueGroupDescription& pqGroupDescription,
        const NKikimrSchemeOp::TDirEntry& selfInfo
    ) {
        Y_UNUSED(selfInfo);

        TString error;
        if (!ValidateShardsCount(*GetProtoRequest(), pqGroupDescription, error) ||
            !ValidateWriteSpeedLimit(*GetProtoRequest(), error, ctx) ||
            !ValidateRetentionLimits(*GetProtoRequest(), groupConfig, Nothing(), error))
        {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::BAD_REQUEST, error, ctx);
        }

        groupConfig.SetTotalGroupCount(GetProtoRequest()->target_shard_count());
        switch (GetProtoRequest()->retention_case()) {
            case Ydb::DataStreams::V1::UpdateStreamRequest::RetentionCase::kRetentionPeriodHours:
                groupConfig.MutablePQTabletConfig()->MutablePartitionConfig()->SetLifetimeSeconds(
                    TDuration::Hours(GetProtoRequest()->retention_period_hours()).Seconds());
                break;
            case Ydb::DataStreams::V1::UpdateStreamRequest::RetentionCase::kRetentionStorageMegabytes:
                groupConfig.MutablePQTabletConfig()->MutablePartitionConfig()->SetStorageLimitBytes(
                    GetProtoRequest()->retention_storage_megabytes() * 1_MB);
                break;
            default: {}
        }

        groupConfig.MutablePQTabletConfig()->MutablePartitionConfig()->SetWriteSpeedInBytesPerSecond(
                    PartitionWriteSpeedInBytesPerSec(GetProtoRequest()->write_quota_kb_per_sec()));
    }

    //-----------------------------------------------------------------------------------------------------------

    class TSetWriteQuotaActor : public TUpdateSchemeActor<TSetWriteQuotaActor, TEvDataStreamsSetWriteQuotaRequest> {
        using TBase = TUpdateSchemeActor<TSetWriteQuotaActor, TEvDataStreamsSetWriteQuotaRequest>;
        using TProtoRequest = typename TBase::TProtoRequest;

    public:
        TSetWriteQuotaActor(NKikimr::NGRpcService::IRequestOpCtx* request)
                : TBase(request, GetRequest<TProtoRequest>(request)->stream_name())
        {
        }

        void Bootstrap(const TActorContext& ctx);
        void ModifyPersqueueConfig(const TActorContext& ctx,
                                   NKikimrSchemeOp::TPersQueueGroupDescription& groupConfig,
                                   const NKikimrSchemeOp::TPersQueueGroupDescription& pqGroupDescription,
                                   const NKikimrSchemeOp::TDirEntry& selfInfo);
    };

    void TSetWriteQuotaActor::Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        SendDescribeProposeRequest(ctx);
        Become(&TBase::StateWork);
    }

    void TSetWriteQuotaActor::ModifyPersqueueConfig(
        const TActorContext& ctx,
        NKikimrSchemeOp::TPersQueueGroupDescription& groupConfig,
        const NKikimrSchemeOp::TPersQueueGroupDescription& pqGroupDescription,
        const NKikimrSchemeOp::TDirEntry& selfInfo
    ) {
        Y_UNUSED(pqGroupDescription);
        Y_UNUSED(selfInfo);

        TString error;
        if (!ValidateWriteSpeedLimit(*GetProtoRequest(), error, ctx)) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::BAD_REQUEST, error, ctx);
        }
        groupConfig.MutablePQTabletConfig()->MutablePartitionConfig()->SetWriteSpeedInBytesPerSecond(GetProtoRequest()->write_quota_kb_per_sec() * 1_KB);
        groupConfig.MutablePQTabletConfig()->MutablePartitionConfig()->SetBurstSize(GetProtoRequest()->write_quota_kb_per_sec() * 1_KB);
    }

    //-----------------------------------------------------------------------------------------------------------

    template<class TEvProto>
    class TSetStreamRetentionPeriodActor : public TUpdateSchemeActor<TSetStreamRetentionPeriodActor<TEvProto>, TEvProto> {
        using TBase = TUpdateSchemeActor<TSetStreamRetentionPeriodActor<TEvProto>, TEvProto>;
        using TProtoRequest = typename TBase::TProtoRequest;

    public:
        TSetStreamRetentionPeriodActor(NKikimr::NGRpcService::IRequestOpCtx* request, bool shouldIncrease)
            : TBase(request, GetRequest<TProtoRequest>(request)->stream_name())
            , ShouldIncrease(shouldIncrease)
        {
        }

        void Bootstrap(const TActorContext& ctx) {
            TBase::Bootstrap(ctx);
            TBase::SendDescribeProposeRequest(ctx);
            TBase::Become(&TBase::StateWork);
        }

        void ModifyPersqueueConfig(
            const TActorContext& ctx,
            NKikimrSchemeOp::TPersQueueGroupDescription& groupConfig,
            const NKikimrSchemeOp::TPersQueueGroupDescription& pqGroupDescription,
            const NKikimrSchemeOp::TDirEntry& selfInfo
        ) {
            Y_UNUSED(pqGroupDescription);
            Y_UNUSED(selfInfo);

            TString error;
            if (!ValidateRetentionLimits(*this->GetProtoRequest(), groupConfig, ShouldIncrease, error)) {
                return this->ReplyWithError(Ydb::StatusIds::BAD_REQUEST,
                                            Ydb::PersQueue::ErrorCode::BAD_REQUEST, error, ctx);
            }
            groupConfig.MutablePQTabletConfig()->MutablePartitionConfig()->SetLifetimeSeconds(
                TInstant::Hours(this->GetProtoRequest()->retention_period_hours()).Seconds());
        }

    private:
        bool ShouldIncrease;
    };

    //-----------------------------------------------------------------------------------

    class TDescribeStreamActor : public TPQGrpcSchemaBase<TDescribeStreamActor, TEvDataStreamsDescribeStreamRequest>
                               , public TCdcStreamCompatible
    {
        using TBase = TPQGrpcSchemaBase<TDescribeStreamActor, TEvDataStreamsDescribeStreamRequest>;
        using TProtoRequest = typename TBase::TProtoRequest;

    public:
        TDescribeStreamActor(NKikimr::NGRpcService::IRequestOpCtx* request);
        ~TDescribeStreamActor() = default;

        void Bootstrap(const NActors::TActorContext& ctx);

        void StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx);
        void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx);

        void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
            if (ev->Get()->Status != NKikimrProto::EReplyStatus::OK) {
                ReplyWithError(Ydb::StatusIds::INTERNAL_ERROR, Ydb::PersQueue::ErrorCode::ERROR,
                                          TStringBuilder() << "Cannot connect to tablet " << ev->Get()->TabletId, ctx);
            }
        }

        void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
            ReplyWithError(Ydb::StatusIds::INTERNAL_ERROR, Ydb::PersQueue::ErrorCode::ERROR,
                                          TStringBuilder() << "Cannot connect to tablet " << ev->Get()->TabletId, ctx);
        }

        void Handle(TEvPersQueue::TEvOffsetsResponse::TPtr& ev, const TActorContext& ctx) {
            for (auto& part : ev->Get()->Record.GetPartResult()) {
                StartEndOffsetsPerPartition[part.GetPartition()] = std::make_pair<ui64, ui64>(part.GetStartOffset(), part.GetEndOffset());
            }
            if (--RequestsInfly == 0) {
                ReplyAndDie(ctx);
            }
        }

        void Die(const TActorContext& ctx) override {
            //close all pipes
            for (auto& pipe : Pipes) {
                NTabletPipe::CloseClient(ctx, pipe);
            }
            TBase::Die(ctx);
        }

    private:
        void ReplyAndDie(const TActorContext& ctx);

        NKikimrSchemeOp::TDirEntry SelfInfo;
        NKikimrSchemeOp::TPersQueueGroupDescription PQGroup;
        std::vector<TActorId> Pipes;
        ui32 RequestsInfly = 0;
        std::map<ui64, std::pair<ui64, ui64>> StartEndOffsetsPerPartition;
    };

    TDescribeStreamActor::TDescribeStreamActor(NKikimr::NGRpcService::IRequestOpCtx* request)
            : TBase(request, GetRequest<TProtoRequest>(request)->stream_name())
    {
    }

    void TDescribeStreamActor::Bootstrap(const NActors::TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        SendDescribeProposeRequest(ctx);
        Become(&TDescribeStreamActor::StateWork);
    }

    void TDescribeStreamActor::StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPersQueue::TEvOffsetsResponse, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            default: TBase::StateWork(ev, ctx);
        }
    }

    void TDescribeStreamActor::HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
        const NSchemeCache::TSchemeCacheNavigate* result = ev->Get()->Request.Get();
        Y_VERIFY(result->ResultSet.size() == 1); // describe only one topic
        const auto& response = result->ResultSet.front();
        const TString path = JoinSeq("/", response.Path);

        if (ReplyIfNotTopic(ev, ctx)) {
            return;
        }

        Y_VERIFY(response.PQGroupInfo);

        PQGroup = response.PQGroupInfo->Description;
        SelfInfo = response.Self->Info;
        std::set<ui64> tabletIds;
        for (auto& partition : PQGroup.GetPartitions()) {
            tabletIds.insert(partition.GetTabletId());
        }
        if (tabletIds.size() == 0) {
            ReplyAndDie(ctx);
        }

        RequestsInfly = tabletIds.size();

        NTabletPipe::TClientConfig clientConfig;
        clientConfig.RetryPolicy = {
            .RetryLimitCount = 6,
            .MinRetryTime = TDuration::MilliSeconds(10),
            .MaxRetryTime = TDuration::MilliSeconds(100),
            .BackoffMultiplier = 2,
            .DoFirstRetryInstantly = true
        };

        for (auto& tabletId : tabletIds) {
            Pipes.push_back(ctx.Register(NTabletPipe::CreateClient(ctx.SelfID, tabletId, clientConfig)));
            TAutoPtr<TEvPersQueue::TEvOffsets> req(new TEvPersQueue::TEvOffsets);
            NTabletPipe::SendData(ctx, Pipes.back(), req.Release());
        }
    }

    void TDescribeStreamActor::ReplyAndDie(const TActorContext& ctx) {
        Ydb::DataStreams::V1::DescribeStreamResult result;

        auto& pqConfig = PQGroup.GetPQTabletConfig();
        ui32 writeSpeed = pqConfig.GetPartitionConfig().GetWriteSpeedInBytesPerSecond() / 1_KB;
        auto& description = *result.mutable_stream_description();
        description.set_stream_name(GetProtoRequest()->stream_name());
        ui32 retentionPeriodHours = TInstant::Seconds(pqConfig.GetPartitionConfig().GetLifetimeSeconds()).Hours();
        description.set_retention_period_hours(retentionPeriodHours);
        description.set_write_quota_kb_per_sec(writeSpeed);
        if (SelfInfo.GetCreateFinished()) {
            description.set_stream_status(Ydb::DataStreams::V1::StreamDescription::ACTIVE);
        } else {
            description.set_stream_status(Ydb::DataStreams::V1::StreamDescription::CREATING);
        }

        bool startShardFound = GetProtoRequest()->exclusive_start_shard_id().empty();
        description.set_has_more_shards(false);

        description.set_owner(SelfInfo.GetOwner());
        description.set_stream_creation_timestamp(TInstant::MilliSeconds(SelfInfo.GetCreateStep()).Seconds());

        int limit = GetProtoRequest()->limit() == 0 ? 100 : GetProtoRequest()->limit();

        for (uint32_t i = 0; i < (uint32_t)PQGroup.GetPartitions().size(); ++i) {
            ui32 partitionId = PQGroup.GetPartitions(i).GetPartitionId();
            TString shardName = GetShardName(partitionId);
            if (shardName == GetProtoRequest()->exclusive_start_shard_id()) {
                startShardFound = true;
            } else if (startShardFound) {
                if (description.shards_size() >= limit) {
                    description.set_has_more_shards(true);
                    break;
                } else {
                    auto* shard = description.add_shards();
                    shard->set_shard_id(shardName);
                    auto* rangeProto = shard->mutable_hash_key_range();
                    auto range = RangeFromShardNumber(partitionId, PQGroup.GetPartitions().size());
                    rangeProto->set_starting_hash_key(Uint128ToDecimalString(range.Start));
                    rangeProto->set_ending_hash_key(Uint128ToDecimalString(range.End));
                    auto it = StartEndOffsetsPerPartition.find(partitionId);
                    if (it != StartEndOffsetsPerPartition.end()) {
                        auto* rangeProto = shard->mutable_sequence_number_range();
                        rangeProto->set_starting_sequence_number(TStringBuilder() << it->second.first);
                    }
                }
            }
        }
        if (!startShardFound) {
            return ReplyWithResult(Ydb::StatusIds::BAD_REQUEST, ctx);
        }
        return ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
    }


    //-----------------------------------------------------------------------------------

    class TListStreamsActor : public TRpcSchemeRequestActor<TListStreamsActor, NKikimr::NGRpcService::TEvDataStreamsListStreamsRequest> {
        using TBase = TRpcSchemeRequestActor<TListStreamsActor, TEvDataStreamsListStreamsRequest>;

    public:
        TListStreamsActor(NKikimr::NGRpcService::IRequestOpCtx* request);
        ~TListStreamsActor() = default;

        void Bootstrap(const NActors::TActorContext& ctx);

        void StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx);
        void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx);

    protected:
        void SendNavigateRequest(const TActorContext& ctx, const TString& path);
        void SendPendingRequests(const TActorContext& ctx);
        void SendResponse(const TActorContext& ctx);

        void ReplyWithError(Ydb::StatusIds::StatusCode status, Ydb::PersQueue::ErrorCode::ErrorCode pqStatus,
                            const TString& messageText, const NActors::TActorContext& ctx) {
            this->Request_->RaiseIssue(FillIssue(messageText, pqStatus));
            this->Request_->ReplyWithYdbStatus(status);
            this->Die(ctx);
        }

    private:
        static constexpr ui32 MAX_IN_FLIGHT = 5;

        ui32 RequestsInFlight = 0;
        std::vector<std::unique_ptr<TEvTxProxySchemeCache::TEvNavigateKeySet>> WaitingList;
        std::vector<TString> Topics;
    };

    TListStreamsActor::TListStreamsActor(NKikimr::NGRpcService::IRequestOpCtx* request)
        : TBase(request)
    {
    }

    void TListStreamsActor::Bootstrap(const NActors::TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        if (!Request_->GetDatabaseName()) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::BAD_REQUEST,
                                          "Request without dabase is forbiden", ctx);
        }

        if (this->Request_->GetInternalToken().empty()) {
            if (AppData(ctx)->PQConfig.GetRequireCredentialsInNewProtocol()) {
                return ReplyWithError(Ydb::StatusIds::UNAUTHORIZED, Ydb::PersQueue::ErrorCode::ACCESS_DENIED,
                                          "Unauthenticated access is forbidden, please provide credentials", ctx);
            }
        }

        SendNavigateRequest(ctx, *Request_->GetDatabaseName());
        Become(&TListStreamsActor::StateWork);
    }

    void TListStreamsActor::SendPendingRequests(const TActorContext& ctx) {
        if (RequestsInFlight < MAX_IN_FLIGHT && WaitingList.size() > 0) {
            ctx.Send(MakeSchemeCacheID(), WaitingList.back().release());
            WaitingList.pop_back();
            RequestsInFlight++;
        }
    }

    void TListStreamsActor::SendResponse(const TActorContext& ctx) {
        Y_ENSURE(WaitingList.size() == 0 && RequestsInFlight == 0);

        for (TString& topic : Topics) {
            topic = TFsPath(topic).RelativePath(*Request_->GetDatabaseName()).GetPath();
        }
        Sort(Topics.begin(), Topics.end());
        Ydb::DataStreams::V1::ListStreamsResult result;

        int limit = GetProtoRequest()->limit() == 0 ? 100 : GetProtoRequest()->limit();

        if (limit > 10000) {
            Request_->ReplyWithYdbStatus(Ydb::StatusIds::BAD_REQUEST);
            return Die(ctx);
        }

        result.set_has_more_streams(false);
        for (const auto& streamName : Topics) {
            if (GetProtoRequest()->exclusive_start_stream_name().empty()
                || GetProtoRequest()->exclusive_start_stream_name() < streamName)
            {
                if (result.stream_names().size() >= limit) {
                    result.set_has_more_streams(true);
                    break;
                }
                result.add_stream_names(streamName);
            }
        }

        Request_->SendResult(result, Ydb::StatusIds::SUCCESS);
        Die(ctx);
    }

    void TListStreamsActor::SendNavigateRequest(const TActorContext& ctx, const TString &path) {
        auto schemeCacheRequest = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
        schemeCacheRequest->DatabaseName = Request().GetDatabaseName().GetRef();
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Path = NKikimr::SplitPath(path);

        if (!this->Request_->GetInternalToken().empty()) {
            schemeCacheRequest->UserToken = new NACLib::TUserToken(this->Request_->GetInternalToken());
        }

        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;
        schemeCacheRequest->ResultSet.emplace_back(entry);
        WaitingList.push_back(std::make_unique<TEvTxProxySchemeCache::TEvNavigateKeySet>(schemeCacheRequest.release()));
        SendPendingRequests(ctx);
    }

    void TListStreamsActor::StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            default: TBase::StateWork(ev, ctx);
        }
    }

    void TListStreamsActor::Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
        const NSchemeCache::TSchemeCacheNavigate* navigate = ev->Get()->Request.Get();
        for (const auto& entry : navigate->ResultSet) {
            if (entry.Kind == NSchemeCache::TSchemeCacheNavigate::EKind::KindPath
                || entry.Kind == NSchemeCache::TSchemeCacheNavigate::EKind::KindSubdomain)
            {
                Y_ENSURE(entry.ListNodeEntry, "ListNodeEntry is zero");
                for (const auto& child : entry.ListNodeEntry->Children) {
                    TString childFullPath = JoinPath({JoinPath(entry.Path), child.Name});
                    switch (child.Kind) {
                        case NSchemeCache::TSchemeCacheNavigate::EKind::KindPath:
                            if (GetProtoRequest()->recurse()) {
                                SendNavigateRequest(ctx, childFullPath);
                            }
                            break;
                        case NSchemeCache::TSchemeCacheNavigate::EKind::KindTopic:
                            Topics.push_back(childFullPath);
                            break;
                        default:
                            break;
                            // ignore all other types
                    }
                }
            }
        }
        RequestsInFlight--;
        SendPendingRequests(ctx);
        if (RequestsInFlight == 0) {
            SendResponse(ctx);
        }
    }

    //-----------------------------------------------------------------------------------

    class TListStreamConsumersActor : public TPQGrpcSchemaBase<TListStreamConsumersActor, NKikimr::NGRpcService::TEvDataStreamsListStreamConsumersRequest>
                                    , public TCdcStreamCompatible
    {
        using TBase = TPQGrpcSchemaBase<TListStreamConsumersActor, TEvDataStreamsListStreamConsumersRequest>;
        using TProtoRequest = typename TBase::TProtoRequest;

    public:
        TListStreamConsumersActor(NKikimr::NGRpcService::IRequestOpCtx* request);
        ~TListStreamConsumersActor() = default;

        void Bootstrap(const NActors::TActorContext& ctx);
        void StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx);
        void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx);

    protected:
        void SendResponse(const TActorContext& ctx, const std::vector<std::pair<TString, ui64>>& readRules, ui32 leftToRead);

    private:
        static constexpr ui32 MAX_MAX_RESULTS     = 10000;
        static constexpr ui32 MIN_MAX_RESULTS     = 1;
        static constexpr ui32 DEFAULT_MAX_RESULTS = 100;

        TString StreamArn;
        ui32 MaxResults = DEFAULT_MAX_RESULTS;
        TNextToken NextToken;
    };
    TListStreamConsumersActor::TListStreamConsumersActor(NKikimr::NGRpcService::IRequestOpCtx* request)
        : TBase(request, TNextToken(GetRequest<TProtoRequest>(request)->next_token()).IsValid() ?
                         TNextToken(GetRequest<TProtoRequest>(request)->next_token()).GetStreamArn() :
                         GetRequest<TProtoRequest>(request)->stream_arn())
        , NextToken(GetRequest<TProtoRequest>(request)->next_token())
    {
        if (GetProtoRequest()->next_token().empty()) {
            StreamArn = GetProtoRequest()->stream_arn();
            MaxResults = GetProtoRequest()->max_results();
            NextToken = TNextToken(StreamArn, 0, MaxResults, TInstant::Now().MilliSeconds());
        } else {
            StreamArn = NextToken.GetStreamArn();
            MaxResults = NextToken.GetMaxResults();
        }
    }

    void TListStreamConsumersActor::Bootstrap(const NActors::TActorContext& ctx) {
        TBase::Bootstrap(ctx);

        auto maxResultsInRange = MIN_MAX_RESULTS <= MaxResults && MaxResults <= MAX_MAX_RESULTS;
        if (!maxResultsInRange) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::ERROR,
                                  TStringBuilder() << "Requested max_result value '" << MaxResults <<
                                  "' is out of range [" << MIN_MAX_RESULTS << ", " << MAX_MAX_RESULTS <<
                                  "]", ctx);
        }

        if (!NextToken.IsValid()) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::ERROR,
                                  TStringBuilder() << "Provided NextToken has expired or malformed", ctx);
        }
        SendDescribeProposeRequest(ctx);
        Become(&TListStreamConsumersActor::StateWork);
    }

    void TListStreamConsumersActor::StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) {
        switch (ev->GetTypeRewrite()) {
            default: TBase::StateWork(ev, ctx);
        }
    }

    void TListStreamConsumersActor::HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
        const NSchemeCache::TSchemeCacheNavigate* result = ev->Get()->Request.Get();
        Y_VERIFY(result->ResultSet.size() == 1); // describe only one topic

        if (ReplyIfNotTopic(ev, ctx)) {
            return;
        }

        std::vector<std::pair<TString, ui64>> readRules;
        ui32 leftToRead{0};
        const auto& response = result->ResultSet.front();
        const auto& pqGroupDescription = response.PQGroupInfo->Description;
        const auto& streamReadRulesNames = pqGroupDescription.GetPQTabletConfig().GetReadRules();
        const auto& streamReadRulesReadFromTimestamps = pqGroupDescription.GetPQTabletConfig().GetReadFromTimestampsMs();
        const auto alreadyRead = NextToken.GetAlreadyRead();

        if (alreadyRead > (ui32)streamReadRulesNames.size()) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::ERROR,
                                  TStringBuilder() << "Provided next_token is malformed - " <<
                                  "everything is already read", ctx);
        }

        const auto rulesToRead = std::min(streamReadRulesNames.size() - alreadyRead, MaxResults);
        readRules.reserve(rulesToRead);
        auto itName = streamReadRulesNames.begin() + alreadyRead;
        auto itTs = streamReadRulesReadFromTimestamps.begin() + alreadyRead;
        for (auto i = rulesToRead; i > 0; --i, ++itName, ++itTs) {
            readRules.push_back({*itName, *itTs});
        }
        leftToRead = streamReadRulesNames.size() - alreadyRead - rulesToRead;

        SendResponse(ctx, readRules, leftToRead);
    }

    void TListStreamConsumersActor::SendResponse(const TActorContext& ctx, const std::vector<std::pair<TString, ui64>>& readRules, ui32 leftToRead) {
        Ydb::DataStreams::V1::ListStreamConsumersResult result;

        for (auto& readRule : readRules) {
            auto consumer = result.Addconsumers();
            consumer->set_consumer_name(readRule.first);
            consumer->set_consumer_creation_timestamp(readRule.second);
            consumer->set_consumer_status(Ydb::DataStreams::V1::ConsumerDescription_ConsumerStatus_ACTIVE);
            // TODO: consumer->set_consumer_arn();
        }

        if (leftToRead > 0) {
            TNextToken token(StreamArn, NextToken.GetAlreadyRead() + readRules.size(), MaxResults, TInstant::Now().MilliSeconds());
            result.set_next_token(token.Serialize());
        }

        Request_->SendResult(result, Ydb::StatusIds::SUCCESS);
        Die(ctx);
    }

    //-----------------------------------------------------------------------------------------

    class TRegisterStreamConsumerActor : public TUpdateSchemeActor<TRegisterStreamConsumerActor, NKikimr::NGRpcService::TEvDataStreamsRegisterStreamConsumerRequest>
                                       , public TCdcStreamCompatible
    {
        using TBase = TUpdateSchemeActor<TRegisterStreamConsumerActor, TEvDataStreamsRegisterStreamConsumerRequest>;
        using TProtoRequest = typename TBase::TProtoRequest;

    public:
        TRegisterStreamConsumerActor(NKikimr::NGRpcService::IRequestOpCtx* request);
        ~TRegisterStreamConsumerActor() = default;

        void Bootstrap(const NActors::TActorContext& ctx);
        void ModifyPersqueueConfig(const TActorContext& ctx,
                                   NKikimrSchemeOp::TPersQueueGroupDescription& groupConfig,
                                   const NKikimrSchemeOp::TPersQueueGroupDescription& pqGroupDescription,
                                   const NKikimrSchemeOp::TDirEntry& selfInfo);
        void ReplyNotifyTxCompletionResult(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev, const TActorContext& ctx) override;

    private:
        TString ConsumerName;
    };
    TRegisterStreamConsumerActor::TRegisterStreamConsumerActor(NKikimr::NGRpcService::IRequestOpCtx* request)
        : TBase(request, GetRequest<TProtoRequest>(request)->stream_arn())
        , ConsumerName(GetRequest<TProtoRequest>(request)->consumer_name())
    {
    }

    void TRegisterStreamConsumerActor::Bootstrap(const NActors::TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        SendDescribeProposeRequest(ctx);
        Become(&TRegisterStreamConsumerActor::StateWork);
    }

    void TRegisterStreamConsumerActor::ModifyPersqueueConfig(
        const TActorContext& ctx,
        NKikimrSchemeOp::TPersQueueGroupDescription& groupConfig,
        const NKikimrSchemeOp::TPersQueueGroupDescription& pqGroupDescription,
        const NKikimrSchemeOp::TDirEntry& selfInfo
    ) {
        Y_UNUSED(pqGroupDescription);

        auto* pqConfig = groupConfig.MutablePQTabletConfig();
        Ydb::PersQueue::V1::TopicSettings::ReadRule readRule;
        readRule.set_consumer_name(ConsumerName);
        readRule.set_supported_format(Ydb::PersQueue::V1::TopicSettings_Format_FORMAT_BASE);
        readRule.set_starting_message_timestamp_ms(TInstant::Now().MilliSeconds());
        readRule.set_important(false);
        readRule.set_service_type(YDS_SERVICE_TYPE);

        if (readRule.version() == 0) {
            readRule.set_version(selfInfo.GetVersion().GetPQVersion());
        }
        auto serviceTypes = GetSupportedClientServiceTypes(ctx);
        TString error = AddReadRuleToConfig(pqConfig, readRule, serviceTypes, ctx);
        bool hasDuplicates = false;
        if (error.Empty()) {
            hasDuplicates = CheckReadRulesConfig(*pqConfig, serviceTypes, error, ctx);
        }

        if (!error.Empty()) {
            return ReplyWithError(hasDuplicates ? Ydb::StatusIds::ALREADY_EXISTS : Ydb::StatusIds::BAD_REQUEST,
                                  hasDuplicates ? Ydb::PersQueue::ErrorCode::OK : Ydb::PersQueue::ErrorCode::BAD_REQUEST, error, ctx);
        }
    }

    void TRegisterStreamConsumerActor::ReplyNotifyTxCompletionResult(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ev);
        Ydb::DataStreams::V1::RegisterStreamConsumerResult result;
        auto consumer = result.Mutableconsumer();
        consumer->set_consumer_name(ConsumerName);
        consumer->set_consumer_creation_timestamp(std::chrono::seconds(std::time(nullptr)).count());
        // TODO: consumer->set_consumer_arn();
        consumer->set_consumer_status(Ydb::DataStreams::V1::ConsumerDescription_ConsumerStatus_ACTIVE);
        Request_->SendResult(result, Ydb::StatusIds::SUCCESS);
        Die(ctx);
    }

    //-----------------------------------------------------------------------------------------

    class TDeregisterStreamConsumerActor : public TUpdateSchemeActor<TDeregisterStreamConsumerActor, NKikimr::NGRpcService::TEvDataStreamsDeregisterStreamConsumerRequest>
                                         , public TCdcStreamCompatible
    {
        using TBase = TUpdateSchemeActor<TDeregisterStreamConsumerActor, TEvDataStreamsDeregisterStreamConsumerRequest>;
        using TProtoRequest = typename TBase::TProtoRequest;

    public:
        TDeregisterStreamConsumerActor(NKikimr::NGRpcService::IRequestOpCtx* request);
        ~TDeregisterStreamConsumerActor() = default;

        void Bootstrap(const NActors::TActorContext& ctx);
        void ModifyPersqueueConfig(const TActorContext& ctx,
                                   NKikimrSchemeOp::TPersQueueGroupDescription& groupConfig,
                                   const NKikimrSchemeOp::TPersQueueGroupDescription& pqGroupDescription,
                                   const NKikimrSchemeOp::TDirEntry& selfInfo);

    private:
        TString ConsumerName;
    };
    TDeregisterStreamConsumerActor::TDeregisterStreamConsumerActor(NKikimr::NGRpcService::IRequestOpCtx* request)
        : TBase(request, GetRequest<TProtoRequest>(request)->stream_arn())
        , ConsumerName(GetRequest<TProtoRequest>(request)->consumer_name())
    {
    }

    void TDeregisterStreamConsumerActor::Bootstrap(const NActors::TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        SendDescribeProposeRequest(ctx);
        Become(&TDeregisterStreamConsumerActor::StateWork);
    }

    void TDeregisterStreamConsumerActor::ModifyPersqueueConfig(
        const TActorContext& ctx,
        NKikimrSchemeOp::TPersQueueGroupDescription& groupConfig,
        const NKikimrSchemeOp::TPersQueueGroupDescription& pqGroupDescription,
        const NKikimrSchemeOp::TDirEntry& selfInfo
    ) {
        Y_UNUSED(selfInfo);
        auto error = RemoveReadRuleFromConfig(
            groupConfig.MutablePQTabletConfig(),
            pqGroupDescription.GetPQTabletConfig(),
            GetProtoRequest()->consumer_name(),
            ctx
        );
        if (!error.Empty()) {
            return ReplyWithError(Ydb::StatusIds::NOT_FOUND, Ydb::PersQueue::ErrorCode::BAD_REQUEST, error, ctx);
        }
    }

    //-----------------------------------------------------------------------------------------

    class TGetShardIteratorActor : public TPQGrpcSchemaBase<TGetShardIteratorActor, NKikimr::NGRpcService::TEvDataStreamsGetShardIteratorRequest>
                                 , public TCdcStreamCompatible
    {
        using TBase = TPQGrpcSchemaBase<TGetShardIteratorActor, TEvDataStreamsGetShardIteratorRequest>;
        using TProtoRequest = typename TBase::TProtoRequest;

    public:
        TGetShardIteratorActor(NKikimr::NGRpcService::IRequestOpCtx* request);
        ~TGetShardIteratorActor() = default;

        void Bootstrap(const NActors::TActorContext& ctx);
        void StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx);
        void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx);


    private:
        using TIteratorType = Ydb::DataStreams::V1::ShardIteratorType;

        void SendResponse(const TActorContext& ctx, const TShardIterator& shardIt);
        std::optional<ui32> SequenceNumberToInt(const TString& sequenceNumberStr);

        TString StreamName;
        TString ShardId;
        TIteratorType IteratorType;
        ui32 SequenceNumber;
        ui64 ReadTimestampMs;
    };

    TGetShardIteratorActor::TGetShardIteratorActor(NKikimr::NGRpcService::IRequestOpCtx* request)
    : TBase(request, GetRequest<TProtoRequest>(request)->stream_name())
    , StreamName{GetRequest<TProtoRequest>(request)->stream_name()}
    , ShardId{GetRequest<TProtoRequest>(request)->shard_id()}
    , IteratorType{GetRequest<TProtoRequest>(request)->shard_iterator_type()}
    , SequenceNumber{0}
    , ReadTimestampMs{0}
    {
    }

    void TGetShardIteratorActor::Bootstrap(const NActors::TActorContext& ctx) {
        TBase::Bootstrap(ctx);

        switch (IteratorType) {
        case TIteratorType::AFTER_SEQUENCE_NUMBER:
        case TIteratorType::AT_SEQUENCE_NUMBER: {
            auto sn = SequenceNumberToInt(GetProtoRequest()->starting_sequence_number());
            if (!sn) {
                return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::BAD_REQUEST,
                                      TStringBuilder() << "Malformed sequence number", ctx);
            }
            SequenceNumber = sn.value() + (IteratorType == TIteratorType::AFTER_SEQUENCE_NUMBER ? 1u : 0u);
            }
            break;
        case TIteratorType::AT_TIMESTAMP:
            if (GetProtoRequest()->timestamp() == 0 ||
                GetProtoRequest()->timestamp() > static_cast<i64>(TInstant::Now().MilliSeconds())) {
                return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::BAD_REQUEST,
                                      TStringBuilder() << "Shard iterator type is AT_TIMESTAMP, " <<
                                      "but timestamp is either missed or too old or in future", ctx);
            }
            ReadTimestampMs = GetProtoRequest()->timestamp();
            break;
        case TIteratorType::TRIM_HORIZON:
            ReadTimestampMs = 0;
            break;
        case TIteratorType::LATEST:
            ReadTimestampMs = TInstant::Now().MilliSeconds();
            break;
        default:
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::BAD_REQUEST,
                                  TStringBuilder() << "Shard iterator type '" <<
                                  (ui32)IteratorType << "' is not known", ctx);

        }

        SendDescribeProposeRequest(ctx);
        Become(&TGetShardIteratorActor::StateWork);
    }

    void TGetShardIteratorActor::StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) {
        switch (ev->GetTypeRewrite()) {
        default: TBase::StateWork(ev, ctx);
        }
    }

    void TGetShardIteratorActor::HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
        if (ReplyIfNotTopic(ev, ctx)) {
            return;
        }

        const NSchemeCache::TSchemeCacheNavigate* navigate = ev->Get()->Request.Get();
        auto topicInfo = navigate->ResultSet.begin();
        StreamName = NKikimr::CanonizePath(topicInfo->Path);
        if (AppData(ctx)->PQConfig.GetRequireCredentialsInNewProtocol()) {
            NACLib::TUserToken token(this->Request_->GetInternalToken());

            if (!topicInfo->SecurityObject->CheckAccess(NACLib::EAccessRights::SelectRow,
                                                        token)) {
                return this->ReplyWithError(Ydb::StatusIds::UNAUTHORIZED,
                                            Ydb::PersQueue::ErrorCode::ACCESS_DENIED,
                                            TStringBuilder() << "Access to stream "
                                            << this->GetProtoRequest()->stream_name()
                                            << " is denied for subject "
                                            << token.GetUserSID(), ctx);
            }
        }

        const auto& partitions = topicInfo->PQGroupInfo->Description.GetPartitions();
        for (auto& partition : partitions) {
            auto partitionId = partition.GetPartitionId();
            TString shardName = GetShardName(partitionId);
            if (shardName == ShardId) {
                if (topicInfo->ShowPrivatePath) {
                    SendResponse(ctx, TShardIterator::Cdc(StreamName, StreamName, partitionId, ReadTimestampMs, SequenceNumber));
                } else {
                    SendResponse(ctx, TShardIterator::Common(StreamName, StreamName, partitionId, ReadTimestampMs, SequenceNumber));
                }
                return;
            }
        }

        ReplyWithError(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::ERROR,
                       TStringBuilder() << "No such shard: " << ShardId, ctx);
    }

    void TGetShardIteratorActor::SendResponse(const TActorContext& ctx, const TShardIterator& shardIt) {
        Ydb::DataStreams::V1::GetShardIteratorResult result;
        result.set_shard_iterator(shardIt.Serialize());
        Request_->SendResult(result, Ydb::StatusIds::SUCCESS);
        Die(ctx);
    }

    std::optional<ui32> TGetShardIteratorActor::SequenceNumberToInt(const TString& sequenceNumberStr) {
        try {
            return std::stoi(sequenceNumberStr.c_str());
        } catch(...) {
            return std::nullopt;
        }
    }

    //-----------------------------------------------------------------------------------

    class TGetRecordsActor : public TPQGrpcSchemaBase<TGetRecordsActor, TEvDataStreamsGetRecordsRequest>
                           , public TCdcStreamCompatible
    {
        using TBase = TPQGrpcSchemaBase<TGetRecordsActor, TEvDataStreamsGetRecordsRequest>;
        using TProtoRequest = typename TBase::TProtoRequest;

        static constexpr ui32 READ_TIMEOUT_MS = 150;
        static constexpr i32 MAX_LIMIT = 10000;

    public:
        TGetRecordsActor(NKikimr::NGRpcService::IRequestOpCtx* request);
        ~TGetRecordsActor() = default;

        void Bootstrap(const NActors::TActorContext& ctx);
        void StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx);
        void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx);
        void Handle(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx);
        void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx);
        void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx);
        void Die(const TActorContext& ctx) override;

    private:
        void SendReadRequest(const TActorContext& ctx);
        void SendResponse(const TActorContext& ctx,
                          const std::vector<Ydb::DataStreams::V1::Record>& records,
                          ui64 millisBehindLatestMs);

        TShardIterator ShardIterator;
        TString StreamName;
        ui64 TabletId;
        i32 Limit;
        TActorId PipeClient;
    };

    TGetRecordsActor::TGetRecordsActor(NKikimr::NGRpcService::IRequestOpCtx* request)
        : TBase(request, TShardIterator(GetRequest<TProtoRequest>(request)->shard_iterator()).IsValid()
                ? TShardIterator(GetRequest<TProtoRequest>(request)->shard_iterator()).GetStreamName()
                : "undefined")
        , ShardIterator{GetRequest<TProtoRequest>(request)->shard_iterator()}
        , StreamName{ShardIterator.IsValid() ? ShardIterator.GetStreamName() : "undefined"}
        , TabletId{0}
        , Limit{GetRequest<TProtoRequest>(request)->limit()}
    {
    }

    void TGetRecordsActor::Bootstrap(const NActors::TActorContext& ctx) {
        TBase::Bootstrap(ctx);

        if (!ShardIterator.IsValid()) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::ERROR,
                                  TStringBuilder() << "Provided shard iterator is malformed or expired", ctx);
        }

        Limit = Limit == 0 ? MAX_LIMIT : Limit;
        if (Limit < 1 || Limit > MAX_LIMIT) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::ERROR,
                                  TStringBuilder() << "Limit '" << Limit << "' is out of bounds [1; " << MAX_LIMIT << "]", ctx);
        }

        SendDescribeProposeRequest(ctx, ShardIterator.IsCdcTopic());
        Become(&TGetRecordsActor::StateWork);
    }

    void TGetRecordsActor::SendReadRequest(const TActorContext& ctx) {
        NTabletPipe::TClientConfig clientConfig;
        clientConfig.RetryPolicy = {
            .RetryLimitCount = 6,
            .MinRetryTime = TDuration::MilliSeconds(10),
            .MaxRetryTime = TDuration::MilliSeconds(100),
            .BackoffMultiplier = 2,
            .DoFirstRetryInstantly = true
        };
        PipeClient = ctx.RegisterWithSameMailbox(
            NTabletPipe::CreateClient(ctx.SelfID, TabletId, clientConfig)
        );

        NKikimrClient::TPersQueueRequest request;
        request.MutablePartitionRequest()->SetTopic(this->GetTopicPath(ctx));
        request.MutablePartitionRequest()->SetPartition(ShardIterator.GetShardId());
        ActorIdToProto(PipeClient, request.MutablePartitionRequest()->MutablePipeClient());

        auto cmdRead = request.MutablePartitionRequest()->MutableCmdRead();
        cmdRead->SetClientId(NKikimr::NPQ::CLIENTID_WITHOUT_CONSUMER);
        cmdRead->SetCount(Limit);
        cmdRead->SetOffset(ShardIterator.GetSequenceNumber());
        cmdRead->SetReadTimestampMs(ShardIterator.GetReadTimestamp());
        cmdRead->SetTimeoutMs(READ_TIMEOUT_MS);
        cmdRead->SetExternalOperation(true);

        TAutoPtr<TEvPersQueue::TEvRequest> req(new TEvPersQueue::TEvRequest);
        req->Record.Swap(&request);
        NTabletPipe::SendData(ctx, PipeClient, req.Release());
    }

    void TGetRecordsActor::StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPersQueue::TEvResponse, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
        default: TBase::StateWork(ev, ctx);
        }
    }

    void TGetRecordsActor::HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev,
                                  const TActorContext& ctx) {
        const auto &result = ev->Get()->Request.Get();
        const auto response = result->ResultSet.front();

        if (AppData(ctx)->PQConfig.GetRequireCredentialsInNewProtocol()) {
            NACLib::TUserToken token(this->Request_->GetInternalToken());

            if (!response.SecurityObject->CheckAccess(NACLib::EAccessRights::SelectRow,
                                                      token)) {
                return ReplyWithError(Ydb::StatusIds::UNAUTHORIZED,
                                      Ydb::PersQueue::ErrorCode::ACCESS_DENIED,
                                      TStringBuilder() << "Access to stream "
                                      << ShardIterator.GetStreamName()
                                      << " is denied for subject "
                                      << token.GetUserSID(), ctx);
            }
        }

        if (response.Self->Info.GetPathType() == NKikimrSchemeOp::EPathTypePersQueueGroup) {
            const auto& partitions = response.PQGroupInfo->Description.GetPartitions();
            for (auto& partition : partitions) {
                auto partitionId = partition.GetPartitionId();
                if (partitionId == ShardIterator.GetShardId()) {
                    TabletId = partition.GetTabletId();
                    return SendReadRequest(ctx);
                }
            }
        }

        ReplyWithError(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::ERROR,
                       TStringBuilder() << "No such shard: " << ShardIterator.GetShardId(), ctx);
    }

    void TGetRecordsActor::Handle(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;
        switch (record.GetStatus()) {
            case NMsgBusProxy::MSTATUS_ERROR:
                switch (record.GetErrorCode()) {
                    case NPersQueue::NErrorCode::READ_ERROR_TOO_SMALL_OFFSET:
                    case NPersQueue::NErrorCode::READ_ERROR_TOO_BIG_OFFSET:
                        return SendResponse(ctx, {}, 0);
                    default:
                        return ReplyWithError(ConvertPersQueueInternalCodeToStatus(record.GetErrorCode()),
                                              Ydb::PersQueue::ErrorCode::ERROR,
                                              record.GetErrorReason(), ctx);
                }
                break;
            default: {}
        }

        ui64 millisBehindLatestMs = 0;
        std::vector<Ydb::DataStreams::V1::Record> records;
        const auto& response = record.GetPartitionResponse();
        if (response.HasCmdReadResult()) {
            const auto& results = response.GetCmdReadResult().GetResult();
            records.reserve(results.size());
            for (auto& r : results) {
                auto proto(NKikimr::GetDeserializedData(r.GetData()));
                Ydb::DataStreams::V1::Record record;
                record.set_data(proto.GetData());
                record.set_timestamp(r.GetCreateTimestampMS());
                record.set_encryption(Ydb::DataStreams::V1::EncryptionType::NONE);
                record.set_partition_key(r.GetPartitionKey());
                record.set_sequence_number(std::to_string(r.GetOffset()).c_str());
                records.push_back(record);
            }
            millisBehindLatestMs = records.size() > 0
                ? TInstant::Now().MilliSeconds() - results.rbegin()->GetWriteTimestampMS()
                : 0;
        }

        SendResponse(ctx, records, millisBehindLatestMs);
    }

    void TGetRecordsActor::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
        if (ev->Get()->Status != NKikimrProto::EReplyStatus::OK) {
            ReplyWithError(Ydb::StatusIds::INTERNAL_ERROR, Ydb::PersQueue::ErrorCode::ERROR,
                           TStringBuilder() << "Cannot connect to tablet " << ev->Get()->TabletId, ctx);
        }
    }

    void TGetRecordsActor::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
        ReplyWithError(Ydb::StatusIds::INTERNAL_ERROR, Ydb::PersQueue::ErrorCode::ERROR,
                       TStringBuilder() << "Cannot connect to tablet " << ev->Get()->TabletId, ctx);
    }

    void TGetRecordsActor::SendResponse(const TActorContext& ctx,
                                        const std::vector<Ydb::DataStreams::V1::Record>& records,
                                        ui64 millisBehindLatestMs) {
        Ydb::DataStreams::V1::GetRecordsResult result;
        for (auto& r : records) {
            auto record = result.add_records();
            *record = r;
        }

        auto timestamp = records.size() > 0 ? records.back().Gettimestamp() + 1
                                            : ShardIterator.GetReadTimestamp();
        auto seqNo = records.size() > 0 ? std::stoi(records.back().Getsequence_number()) + 1
                                        : ShardIterator.GetSequenceNumber();
        TShardIterator shardIterator(ShardIterator.GetStreamName(),
                                     ShardIterator.GetStreamArn(),
                                     ShardIterator.GetShardId(),
                                     timestamp, seqNo, ShardIterator.GetKind());
        result.set_next_shard_iterator(shardIterator.Serialize());
        result.set_millis_behind_latest(millisBehindLatestMs);

        Request_->SendResult(result, Ydb::StatusIds::SUCCESS);
        Die(ctx);
    }

    void TGetRecordsActor::Die(const TActorContext& ctx) {
        NTabletPipe::CloseClient(ctx, PipeClient);
        TBase::Die(ctx);
    }

    //-----------------------------------------------------------------------------------------

    class TListShardsActor : public TPQGrpcSchemaBase<TListShardsActor, NKikimr::NGRpcService::TEvDataStreamsListShardsRequest>
                           , public TCdcStreamCompatible
    {
        using TBase = TPQGrpcSchemaBase<TListShardsActor, TEvDataStreamsListShardsRequest>;
        using TProtoRequest = typename TBase::TProtoRequest;

    public:
        TListShardsActor(NKikimr::NGRpcService::IRequestOpCtx* request);
        ~TListShardsActor() = default;

        void Bootstrap(const NActors::TActorContext& ctx);
        void StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx);
        void Handle(TEvPersQueue::TEvOffsetsResponse::TPtr& ev, const TActorContext& ctx);
        void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx);
        void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx);
        void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev,
                                         const TActorContext& ctx);
        void Die(const TActorContext& ctx) override;

    private:
        using TShardFilter = Ydb::DataStreams::V1::ShardFilter;

        void SendResponse(const TActorContext& ctx);

        static constexpr ui32 MAX_MAX_RESULTS     = 10000;
        static constexpr ui32 MIN_MAX_RESULTS     = 1;
        static constexpr ui32 DEFAULT_MAX_RESULTS = 100;

        TString StreamName;
        TShardFilter ShardFilter;
        TNextToken NextToken;
        ui32 MaxResults = DEFAULT_MAX_RESULTS;
        std::map<ui64, std::pair<ui64, ui64>> StartEndOffsetsPerPartition;
        std::vector<NKikimrSchemeOp::TPersQueueGroupDescription::TPartition> Shards;
        ui32 LeftToRead = 0;
        ui32 AllShardsCount = 0;
        std::atomic<ui32> GotOffsetResponds;
        std::vector<TActorId> Pipes;
    };

    TListShardsActor::TListShardsActor(NKikimr::NGRpcService::IRequestOpCtx* request)
    : TBase(request, GetRequest<TProtoRequest>(request)->stream_name())
    , StreamName{GetProtoRequest()->stream_name()}
    , ShardFilter{GetProtoRequest()->shard_filter()}
    , NextToken{GetProtoRequest()->next_token()}
    , GotOffsetResponds{0}
    {
        if (GetProtoRequest()->next_token().empty()) {
            StreamName = GetProtoRequest()->stream_name();
            MaxResults = GetProtoRequest()->max_results();
            NextToken = TNextToken(StreamName, 0, MaxResults, TInstant::Now().MilliSeconds());
        } else {
            StreamName = NextToken.GetStreamArn();
            MaxResults = NextToken.GetMaxResults();
        }
    }

    void TListShardsActor::Bootstrap(const NActors::TActorContext& ctx) {
        TBase::Bootstrap(ctx);

        if (!TShardFilter::ShardFilterType_IsValid(ShardFilter.type())) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::BAD_REQUEST,
                                  TStringBuilder() << "Shard filter '" <<
                                  (ui32)ShardFilter.type() << "' is not known", ctx);
        }

        MaxResults = MaxResults == 0 ? DEFAULT_MAX_RESULTS : MaxResults;
        if (MaxResults > MAX_MAX_RESULTS) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::BAD_REQUEST,
                                  TStringBuilder() << "Max results '" << MaxResults <<
                                  "' is out of bound [" << MIN_MAX_RESULTS << "; " <<
                                  MAX_MAX_RESULTS << "]", ctx);
        }

        if (ShardFilter.type() == TShardFilter::AFTER_SHARD_ID && ShardFilter.shard_id() == "") {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::BAD_REQUEST,
                                  TStringBuilder() << "Shard filter type is AFTER_SHARD_ID," <<
                                  " but no ShardId provided", ctx);
        }

        SendDescribeProposeRequest(ctx);
        Become(&TListShardsActor::StateWork);
    }

    void TListShardsActor::StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPersQueue::TEvOffsetsResponse, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
        default: TBase::StateWork(ev, ctx);
        }
    }

    void TListShardsActor::HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
        if (ReplyIfNotTopic(ev, ctx)) {
            return;
        }

        const NSchemeCache::TSchemeCacheNavigate* navigate = ev->Get()->Request.Get();
        auto topicInfo = navigate->ResultSet.front();
        if (AppData(ctx)->PQConfig.GetRequireCredentialsInNewProtocol()) {
            NACLib::TUserToken token(this->Request_->GetInternalToken());

            if (!topicInfo.SecurityObject->CheckAccess(NACLib::EAccessRights::SelectRow,
                                                        token)) {
                return this->ReplyWithError(Ydb::StatusIds::UNAUTHORIZED,
                                            Ydb::PersQueue::ErrorCode::ACCESS_DENIED,
                                            TStringBuilder() << "Access to stream "
                                            << this->GetProtoRequest()->stream_name()
                                            << " is denied for subject "
                                            << token.GetUserSID(), ctx);
            }
        }

        using TPartition = NKikimrSchemeOp::TPersQueueGroupDescription::TPartition;
        const auto& partitions = topicInfo.PQGroupInfo->Description.GetPartitions();
        TString startingShardId = this->GetProtoRequest()->Getexclusive_start_shard_id();
        ui64 startingTimepoint{0};
        bool onlyOpenShards{true};

        std::map<TShardFilter::ShardFilterType, std::function<bool(const TPartition&)>> filters = {
            {TShardFilter::SHARD_TYPE_UNDEFINED, [&](const TPartition& p) {
                onlyOpenShards = false;
                return GetShardName(p.GetPartitionId()) >= startingShardId;
            }},
            {TShardFilter::AFTER_SHARD_ID, [&](const TPartition& p) {
                startingShardId = ShardFilter.shard_id();
                startingTimepoint = 0;
                onlyOpenShards = false;
                return GetShardName(p.GetPartitionId()) > startingShardId;
            }},
            {TShardFilter::AT_TRIM_HORIZON, [&](const TPartition& p) {
                startingShardId = "0";
                startingTimepoint = 0;
                onlyOpenShards = true;
                return GetShardName(p.GetPartitionId()) >= startingShardId;
            }},
            { TShardFilter::FROM_TRIM_HORIZON, [&](const TPartition& p) {
                startingShardId = "0";
                startingTimepoint = 0;
                onlyOpenShards = false;
                return GetShardName(p.GetPartitionId()) >= startingShardId;
            }},
            {TShardFilter::AT_LATEST, [&](const TPartition& p) {
                startingTimepoint = TInstant::Now().MilliSeconds();
                startingShardId = "0";
                onlyOpenShards = true;
                return GetShardName(p.GetPartitionId()) >= startingShardId;
            }},
            {TShardFilter::AT_TIMESTAMP, [&](const TPartition& p) {
                startingTimepoint = ShardFilter.timestamp();
                startingShardId = "0";
                onlyOpenShards = true;
                return GetShardName(p.GetPartitionId()) >= startingShardId;
            }},
            {TShardFilter::FROM_TIMESTAMP, [&](const TPartition& p) {
                startingTimepoint = ShardFilter.timestamp();
                startingShardId = "0";
                onlyOpenShards = false;
                return GetShardName(p.GetPartitionId()) >= startingShardId;
            }}
        };

        const auto alreadyRead = NextToken.GetAlreadyRead();
        if (alreadyRead > (ui32)partitions.size()) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::ERROR,
                                  TStringBuilder() << "Provided next_token is malformed - "
                                  "everything is already read", ctx);
        }

        const auto shardsToGet = std::min(partitions.size() - alreadyRead, MaxResults);
        i32 lastCopied{0};
        Shards.reserve(shardsToGet);
        AllShardsCount = partitions.size();
        for (auto partition = partitions.begin() + alreadyRead; partition != partitions.end(); ++partition) {
            if (Shards.size() == shardsToGet) {
                break;
            }
            if (filters[ShardFilter.type()](*partition)) {
                Shards.push_back(*partition);
            }
            ++lastCopied;
        }
        auto actuallyRead = lastCopied - alreadyRead;
        LeftToRead = partitions.size() - alreadyRead - actuallyRead;

        if (Shards.size() == 0) {
            return SendResponse(ctx);
        }

        // Send OffsetRequests
        std::set<ui64> tabletIds;
        for (const auto& shard : Shards) {
            tabletIds.insert(shard.GetTabletId());
        }

        NTabletPipe::TClientConfig clientConfig;
        clientConfig.RetryPolicy = {
            .RetryLimitCount = 6,
            .MinRetryTime = TDuration::MilliSeconds(10),
            .MaxRetryTime = TDuration::MilliSeconds(100),
            .BackoffMultiplier = 2,
            .DoFirstRetryInstantly = true
        };

        for (auto& tabletId : tabletIds) {
            Pipes.push_back(ctx.Register(NTabletPipe::CreateClient(ctx.SelfID, tabletId, clientConfig)));
            TAutoPtr<TEvPersQueue::TEvOffsets> req(new TEvPersQueue::TEvOffsets);
            NTabletPipe::SendData(ctx, Pipes.back(), req.Release());
        }
    }

    void TListShardsActor::Handle(TEvPersQueue::TEvOffsetsResponse::TPtr& ev, const TActorContext& ctx) {
        for (auto& part : ev->Get()->Record.GetPartResult()) {
            StartEndOffsetsPerPartition[part.GetPartition()] =
                std::make_pair<ui64, ui64>(part.GetStartOffset(), part.GetEndOffset());
            ++GotOffsetResponds;
        }
        if (GotOffsetResponds == Shards.size()) {
            SendResponse(ctx);
        }
    }

    void TListShardsActor::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
        if (ev->Get()->Status != NKikimrProto::EReplyStatus::OK) {
            ReplyWithError(Ydb::StatusIds::INTERNAL_ERROR, Ydb::PersQueue::ErrorCode::ERROR,
                           TStringBuilder() << "Cannot connect to tablet " << ev->Get()->TabletId, ctx);
        }
    }

    void TListShardsActor::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
        ReplyWithError(Ydb::StatusIds::INTERNAL_ERROR, Ydb::PersQueue::ErrorCode::ERROR,
                       TStringBuilder() << "Cannot connect to tablet " << ev->Get()->TabletId, ctx);
    }

    void TListShardsActor::SendResponse(const TActorContext& ctx) {
        Ydb::DataStreams::V1::ListShardsResult result;
        for (auto& shard : Shards) {
            auto awsShard = result.Addshards();
            // TODO:
            // awsShard->set_parent_shard_id("");
            // awsShard->set_adjacent_parent_shard_id(prevShardName);
            auto range = RangeFromShardNumber(shard.GetPartitionId(), AllShardsCount);
            awsShard->mutable_hash_key_range()->set_starting_hash_key(
                Uint128ToDecimalString(range.Start));
            awsShard->mutable_hash_key_range()->set_ending_hash_key(
                Uint128ToDecimalString(range.End));
            awsShard->mutable_sequence_number_range()->set_starting_sequence_number(
                std::to_string(StartEndOffsetsPerPartition[shard.GetPartitionId()].first));
            awsShard->mutable_sequence_number_range()->set_ending_sequence_number(
                std::to_string(StartEndOffsetsPerPartition[shard.GetPartitionId()].second));
            awsShard->set_shard_id(GetShardName(shard.GetPartitionId()));
        }
        if (LeftToRead > 0) {
            TNextToken token(StreamName, NextToken.GetAlreadyRead() + Shards.size(), MaxResults, TInstant::Now().MilliSeconds());
            result.set_next_token(token.Serialize());
        }
        Request_->SendResult(result, Ydb::StatusIds::SUCCESS);
        Die(ctx);
    }

    void TListShardsActor::Die(const TActorContext& ctx) {
        //close all pipes
        for (auto& pipe : Pipes) {
            NTabletPipe::CloseClient(ctx, pipe);
        }
        TBase::Die(ctx);
    }

    //-----------------------------------------------------------------------------------

    class TDescribeStreamSummaryActor : public TPQGrpcSchemaBase<TDescribeStreamSummaryActor, TEvDataStreamsDescribeStreamSummaryRequest> {
        using TBase = TPQGrpcSchemaBase<TDescribeStreamSummaryActor, TEvDataStreamsDescribeStreamSummaryRequest>;
        using TProtoRequest = typename TBase::TProtoRequest;

    public:
        TDescribeStreamSummaryActor(NKikimr::NGRpcService::IRequestOpCtx* request);
        ~TDescribeStreamSummaryActor() = default;

        void Bootstrap(const NActors::TActorContext& ctx);

        void StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx);
        void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev,
                                         const TActorContext& ctx);

    private:
        void SendResponse(const TActorContext& ctx);

        NKikimrSchemeOp::TDirEntry SelfInfo;
        NKikimrSchemeOp::TPersQueueGroupDescription PQGroup;
    };

    TDescribeStreamSummaryActor::TDescribeStreamSummaryActor(
        NKikimr::NGRpcService::IRequestOpCtx* request
    )
        : TBase(request, GetRequest<TProtoRequest>(request)->stream_name())
    {
    }

    void TDescribeStreamSummaryActor::Bootstrap(const NActors::TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        SendDescribeProposeRequest(ctx);
        Become(&TDescribeStreamSummaryActor::StateWork);
    }

    void TDescribeStreamSummaryActor::StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) {
        switch (ev->GetTypeRewrite()) {
        default: TBase::StateWork(ev, ctx);
        }
    }

    void TDescribeStreamSummaryActor::HandleCacheNavigateResponse(
        TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx
    ) {
        if (ReplyIfNotTopic(ev, ctx)) {
            return;
        }

        const NSchemeCache::TSchemeCacheNavigate* result = ev->Get()->Request.Get();
        Y_VERIFY(result->ResultSet.size() == 1); // describe only one topic
        const auto& response = result->ResultSet.front();
        Y_VERIFY(response.PQGroupInfo);
        const TString path = JoinSeq("/", response.Path);

        PQGroup = response.PQGroupInfo->Description;
        SelfInfo = response.Self->Info;

        SendResponse(ctx);
    }

    void TDescribeStreamSummaryActor::SendResponse(const TActorContext& ctx) {
        Ydb::DataStreams::V1::DescribeStreamSummaryResult result;

        auto& pqConfig = PQGroup.GetPQTabletConfig();
        auto& descriptionSummary = *result.mutable_stream_description_summary();

        descriptionSummary.set_stream_name(GetProtoRequest()->stream_name());
        descriptionSummary.set_stream_arn(GetProtoRequest()->stream_name());
        descriptionSummary.set_key_id("");
        descriptionSummary.set_retention_period_hours(
            TInstant::Seconds(pqConfig.GetPartitionConfig().GetLifetimeSeconds()).Hours()
        );
        descriptionSummary.set_stream_creation_timestamp(
            TInstant::MilliSeconds(SelfInfo.GetCreateStep()).Seconds()
        );
        descriptionSummary.set_stream_status(
            SelfInfo.GetCreateFinished() ? Ydb::DataStreams::V1::StreamDescription::ACTIVE
                                         : Ydb::DataStreams::V1::StreamDescription::CREATING
        );
        descriptionSummary.set_open_shard_count(PQGroup.GetPartitions().size());
        descriptionSummary.set_consumer_count(PQGroup.MutablePQTabletConfig()->GetReadRules().size());
        descriptionSummary.set_encryption_type(Ydb::DataStreams::V1::EncryptionType::NONE);

        Request_->SendResult(result, Ydb::StatusIds::SUCCESS);
        Die(ctx);
    }

    //-----------------------------------------------------------------------------------------

    template<class TEvRequest>
    class TNotImplementedRequestActor : public TRpcSchemeRequestActor<TNotImplementedRequestActor<TEvRequest>, TEvRequest> {
        using TBase = TRpcSchemeRequestActor<TNotImplementedRequestActor, TEvRequest>;

    public:
        TNotImplementedRequestActor(NKikimr::NGRpcService::IRequestOpCtx* request)
            : TBase(request)
        {
        }
        ~TNotImplementedRequestActor() = default;

        void Bootstrap(const NActors::TActorContext& ctx) {
            TBase::Bootstrap(ctx);
            this->Request_->RaiseIssue(FillIssue("Method is not implemented yet", Ydb::PersQueue::ErrorCode::ErrorCode::ERROR));
            this->Request_->ReplyWithYdbStatus(Ydb::StatusIds::UNSUPPORTED);
            this->Die(ctx);
        }
    };
}

namespace NKikimr::NGRpcService {

using namespace NDataStreams::V1;

#define DECLARE_RPC(name) template<> IActor* TEvDataStreams##name##Request::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) { \
    return new T##name##Actor(msg);\
}\
void DoDataStreams##name##Request(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {\
    TActivationContext::AsActorContext().Register(new T##name##Actor(p.release())); \
}

#define DECLARE_RPC_NI(name) template<> IActor* TEvDataStreams##name##Request::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) { \
    return new TNotImplementedRequestActor<NKikimr::NGRpcService::TEvDataStreams##name##Request>(msg);\
}\
void DoDataStreams##name##Request(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {\
    TActivationContext::AsActorContext().Register(new TNotImplementedRequestActor<NKikimr::NGRpcService::TEvDataStreams##name##Request>(p.release()));\
}


DECLARE_RPC(CreateStream);
DECLARE_RPC(DeleteStream);
DECLARE_RPC(DescribeStream);
DECLARE_RPC(PutRecord);
DECLARE_RPC(RegisterStreamConsumer);
DECLARE_RPC(DeregisterStreamConsumer);
DECLARE_RPC_NI(DescribeStreamConsumer);
DECLARE_RPC(ListStreams);
DECLARE_RPC(ListShards);
DECLARE_RPC(PutRecords);
DECLARE_RPC(GetRecords);
DECLARE_RPC(GetShardIterator);
DECLARE_RPC_NI(SubscribeToShard);
DECLARE_RPC_NI(DescribeLimits);
DECLARE_RPC(DescribeStreamSummary);
DECLARE_RPC(UpdateShardCount);
DECLARE_RPC(ListStreamConsumers);
DECLARE_RPC_NI(AddTagsToStream);
DECLARE_RPC_NI(DisableEnhancedMonitoring);
DECLARE_RPC_NI(EnableEnhancedMonitoring);
DECLARE_RPC_NI(ListTagsForStream);
DECLARE_RPC(UpdateStream);
DECLARE_RPC(SetWriteQuota);
DECLARE_RPC_NI(MergeShards);
DECLARE_RPC_NI(RemoveTagsFromStream);
DECLARE_RPC_NI(SplitShard);
DECLARE_RPC_NI(StartStreamEncryption);
DECLARE_RPC_NI(StopStreamEncryption);



void DoDataStreamsDecreaseStreamRetentionPeriodRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    auto* req = dynamic_cast<TEvDataStreamsDecreaseStreamRetentionPeriodRequest*>(p.release());
    Y_VERIFY(req != nullptr, "Wrong using of TGRpcRequestWrapper");
    TActivationContext::AsActorContext().Register(new TSetStreamRetentionPeriodActor<TEvDataStreamsDecreaseStreamRetentionPeriodRequest>(req, false));
}
template<>
IActor* TEvDataStreamsDecreaseStreamRetentionPeriodRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
    return new TSetStreamRetentionPeriodActor<TEvDataStreamsDecreaseStreamRetentionPeriodRequest>(dynamic_cast<TEvDataStreamsDecreaseStreamRetentionPeriodRequest*>(msg), false);
}

void DoDataStreamsIncreaseStreamRetentionPeriodRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    auto* req = dynamic_cast<TEvDataStreamsIncreaseStreamRetentionPeriodRequest*>(p.release());
    Y_VERIFY(req != nullptr, "Wrong using of TGRpcRequestWrapper");
    TActivationContext::AsActorContext().Register(new TSetStreamRetentionPeriodActor<TEvDataStreamsIncreaseStreamRetentionPeriodRequest>(req, true));
}
template<>
IActor* TEvDataStreamsIncreaseStreamRetentionPeriodRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
    return new TSetStreamRetentionPeriodActor<TEvDataStreamsIncreaseStreamRetentionPeriodRequest>(dynamic_cast<TEvDataStreamsIncreaseStreamRetentionPeriodRequest*>(msg), true);
}


}
