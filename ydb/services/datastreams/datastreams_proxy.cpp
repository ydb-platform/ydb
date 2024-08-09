#include "datastreams_proxy.h"
#include "datastreams_codes.h"
#include "put_records_actor.h"
#include "shard_iterator.h"
#include "next_token.h"

#include <ydb/core/grpc_services/service_datastreams.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_deferrable.h>
#include <ydb/core/grpc_services/rpc_scheme_base.h>
#include <ydb/core/persqueue/partition.h>
#include <ydb/core/persqueue/pq_rl_helpers.h>
#include <ydb/core/persqueue/write_meta.h>

#include <ydb/public/api/protos/ydb_topic.pb.h>
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
    const i32 DEFAULT_STREAM_WEEK_RETENTION = TDuration::Days(7).Hours();

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
        bool ValidateShardsCount(const TRequest& req, const NKikimrSchemeOp::TPersQueueGroupDescription& groupConfig, TString& errorText) {
            if (req.target_shard_count() < (i32)groupConfig.GetTotalGroupCount()) {
                errorText = TStringBuilder() << "Shard count must be non-decreasing, old value is "
                                             << groupConfig.GetTotalGroupCount()
                                             << " new value is " << req.target_shard_count();
                return false;
            }
            return true;
        }

        TString ValidatePartitioningSettings(const ::Ydb::DataStreams::V1::PartitioningSettings& s) {
            if (s.auto_partitioning_settings().strategy() == ::Ydb::DataStreams::V1::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_SCALE_UP
                || s.auto_partitioning_settings().strategy() == ::Ydb::DataStreams::V1::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_SCALE_UP_AND_DOWN
                || s.auto_partitioning_settings().strategy() == ::Ydb::DataStreams::V1::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_PAUSED) {

                if (s.min_active_partitions() < 0) {
                    return "min_active_partitions must be great than 0";
                }

                if (s.max_active_partitions() < 0) {
                    return "max_active_partitions must be great than 0";
                }

                if (s.min_active_partitions() > s.max_active_partitions()) {
                    return TStringBuilder() << "max_active_partitions must be great or equals than min_active_partitions but "
                        << s.max_active_partitions() << " less then  " << s.min_active_partitions();
                }

                auto& ws = s.auto_partitioning_settings().partition_write_speed();

                if (ws.up_utilization_percent() && (ws.up_utilization_percent() < 0 || ws.up_utilization_percent() > 100)) {
                    return "up_utilization_percent must be between 0 and 100";
                }
                if (ws.down_utilization_percent() && (ws.down_utilization_percent() < 0 || ws.down_utilization_percent() > 100)) {
                    return "down_utilization_percent must be between 0 and 100";
                }
            }

            return {};
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
        void StateWork(TAutoPtr<IEventHandle>& ev);
        void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
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

    void TCreateStreamActor::HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        Y_UNUSED(ev);
    }


    void TCreateStreamActor::FillProposeRequest(TEvTxUserProxy::TEvProposeTransaction& proposal,
            const TActorContext& ctx, const TString& workingDir, const TString& name)
    {
        NKikimrSchemeOp::TModifyScheme& modifyScheme(*proposal.Record.MutableTransaction()->MutableModifyScheme());

        Ydb::Topic::CreateTopicRequest topicRequest;
        topicRequest.mutable_partitioning_settings()->set_min_active_partitions(GetProtoRequest()->shard_count());
        switch (GetProtoRequest()->retention_case()) {
            case Ydb::DataStreams::V1::CreateStreamRequest::RetentionCase::kRetentionPeriodHours:
                topicRequest.mutable_retention_period()->set_seconds(
                    TDuration::Hours(GetProtoRequest()->retention_period_hours()).Seconds());
                break;
            case Ydb::DataStreams::V1::CreateStreamRequest::RetentionCase::kRetentionStorageMegabytes:
                topicRequest.set_retention_storage_mb(
                    GetProtoRequest()->retention_storage_megabytes());
                topicRequest.mutable_retention_period()->set_seconds(
                    TDuration::Hours(DEFAULT_STREAM_WEEK_RETENTION).Seconds());
                break;
            default:
                topicRequest.mutable_retention_period()->set_seconds(
                    TDuration::Hours(DEFAULT_STREAM_DAY_RETENTION).Seconds());
        }
        topicRequest.set_partition_write_speed_bytes_per_second(
            PartitionWriteSpeedInBytesPerSec(GetProtoRequest()->write_quota_kb_per_sec()));
        topicRequest.set_partition_write_burst_bytes(
            PartitionWriteSpeedInBytesPerSec(GetProtoRequest()->write_quota_kb_per_sec()));

        if (AppData(ctx)->PQConfig.GetBillingMeteringConfig().GetEnabled()) {
            topicRequest.set_metering_mode(Ydb::Topic::METERING_MODE_REQUEST_UNITS);

            if (GetProtoRequest()->has_stream_mode_details()) {
                switch(GetProtoRequest()->stream_mode_details().stream_mode()) {
                    case Ydb::DataStreams::V1::StreamMode::PROVISIONED:
                        topicRequest.set_metering_mode(Ydb::Topic::METERING_MODE_RESERVED_CAPACITY);
                        break;
                    case Ydb::DataStreams::V1::StreamMode::ON_DEMAND:
                        topicRequest.set_metering_mode(Ydb::Topic::METERING_MODE_REQUEST_UNITS);
                        break;
                    default:
                        return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, static_cast<size_t>(NYds::EErrorCodes::INVALID_ARGUMENT),
                                      "streams can't be created with unknown metering mode", ctx);
                }
            }
        } else {
            if (GetProtoRequest()->has_stream_mode_details()) {
                return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, static_cast<size_t>(NYds::EErrorCodes::INVALID_ARGUMENT),
                              "streams can't be created with metering mode", ctx);
            }
        }

        if (GetProtoRequest()->has_partitioning_settings()) {
            auto r = ValidatePartitioningSettings(GetProtoRequest()->partitioning_settings());
            if (!r.empty()) {
                return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, static_cast<size_t>(NYds::EErrorCodes::INVALID_ARGUMENT), r, ctx);
            }

            auto& s = GetProtoRequest()->partitioning_settings();
            auto* t = topicRequest.mutable_partitioning_settings();
            t->set_min_active_partitions(s.min_active_partitions());
            t->set_max_active_partitions(s.max_active_partitions());

            auto& as = s.auto_partitioning_settings();
            auto* at = t->mutable_auto_partitioning_settings();
            at->set_strategy(static_cast<::Ydb::Topic::AutoPartitioningStrategy>(as.strategy()));

            auto& ws = as.partition_write_speed();
            auto* wt = at->mutable_partition_write_speed();

            wt->mutable_stabilization_window()->CopyFrom(ws.stabilization_window());
            wt->set_up_utilization_percent(ws.up_utilization_percent());
            wt->set_down_utilization_percent(ws.down_utilization_percent());
        }

        auto pqDescr = modifyScheme.MutableCreatePersQueueGroup();
        if (GetProtoRequest()->retention_case() ==
            Ydb::DataStreams::V1::CreateStreamRequest::RetentionCase::kRetentionStorageMegabytes) {
            modifyScheme.MutableCreatePersQueueGroup()->MutablePQTabletConfig()->
                MutablePartitionConfig()->SetLifetimeSeconds(TDuration::Hours(DEFAULT_STREAM_WEEK_RETENTION).Seconds());
        }

        modifyScheme.SetWorkingDir(workingDir);

        pqDescr->SetPartitionPerTablet(1);
        TString error;
        TYdbPqCodes codes = NKikimr::NGRpcProxy::V1::FillProposeRequestImpl(name, topicRequest, modifyScheme, ctx, error,
                                                                      workingDir, proposal.Record.GetDatabaseName());
        if (codes.YdbCode != Ydb::StatusIds::SUCCESS) {
            return ReplyWithError(codes.YdbCode, codes.PQCode, error, ctx);
        }
    }

    void TCreateStreamActor::Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev, const TActorContext& ctx) {
        auto msg = ev->Get();
        const auto status = static_cast<TEvTxUserProxy::TEvProposeTransactionStatus::EStatus>(ev->Get()->Record.GetStatus());
        if (status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete
            && msg->Record.GetSchemeShardStatus() == NKikimrScheme::EStatus::StatusAlreadyExists)
        {
            return ReplyWithError(Ydb::StatusIds::ALREADY_EXISTS,
                                  static_cast<size_t>(NYds::EErrorCodes::IN_USE),
                                  TStringBuilder() << "Stream with name " << GetProtoRequest()->stream_name() << " already exists",
                                  ctx);
        }
        return TBase::TBase::Handle(ev, ctx);
    }

    void TCreateStreamActor::StateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxUserProxy::TEvProposeTransactionStatus, Handle);
            default: TBase::StateWork(ev);
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

        void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);

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

    void TDeleteStreamActor::HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        if (ReplyIfNotTopic(ev)) {
            return;
        }

        const auto& response = ev->Get()->Request.Get()->ResultSet.front();
        const auto& pqGroupDescription = response.PQGroupInfo->Description;

        if (NPQ::ConsumerCount(pqGroupDescription.GetPQTabletConfig()) > 0 && EnforceDeletion == false) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, static_cast<size_t>(NYds::EErrorCodes::IN_USE),
                                  TStringBuilder() << "Stream has registered consumers" <<
                                  "and EnforceConsumerDeletion flag is false", ActorContext());
        }

        SendProposeRequest(ActorContext());
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
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, static_cast<size_t>(NYds::EErrorCodes::INVALID_ARGUMENT), error, ctx);
        }

        groupConfig.SetTotalGroupCount(GetProtoRequest()->target_shard_count());
    }

    //-----------------------------------------------------------------------------------------------------------

    class TUpdateStreamModeActor : public TUpdateSchemeActor<TUpdateStreamModeActor, TEvDataStreamsUpdateStreamModeRequest> {
        using TBase = TUpdateSchemeActor<TUpdateStreamModeActor, TEvDataStreamsUpdateStreamModeRequest>;
        using TProtoRequest = typename TBase::TProtoRequest;
    public:
        TUpdateStreamModeActor(NKikimr::NGRpcService::IRequestOpCtx* request)
            : TBase(request, GetRequest<TProtoRequest>(request)->stream_arn())
        {
        }

        void Bootstrap(const TActorContext& ctx);
        void ModifyPersqueueConfig(const TActorContext& ctx,
                                   NKikimrSchemeOp::TPersQueueGroupDescription& groupConfig,
                                   const NKikimrSchemeOp::TPersQueueGroupDescription& pqGroupDescription,
                                   const NKikimrSchemeOp::TDirEntry& selfInfo);
    };

    void TUpdateStreamModeActor::Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        SendDescribeProposeRequest(ctx);
        Become(&TBase::StateWork);
    }

    void TUpdateStreamModeActor::ModifyPersqueueConfig(
        const TActorContext& ctx,
        NKikimrSchemeOp::TPersQueueGroupDescription& groupConfig,
        const NKikimrSchemeOp::TPersQueueGroupDescription& pqGroupDescription,
        const NKikimrSchemeOp::TDirEntry& selfInfo
    ) {
        Y_UNUSED(selfInfo);
        Y_UNUSED(pqGroupDescription);
        if (!AppData(ctx)->PQConfig.GetBillingMeteringConfig().GetEnabled()) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, static_cast<size_t>(NYds::EErrorCodes::INVALID_ARGUMENT),
                      "streams can't be created with metering mode", ctx);
        }

        switch(GetProtoRequest()->stream_mode_details().stream_mode()) {
            case Ydb::DataStreams::V1::StreamMode::PROVISIONED:
                groupConfig.MutablePQTabletConfig()->SetMeteringMode(NKikimrPQ::TPQTabletConfig::METERING_MODE_RESERVED_CAPACITY);
                break;
            case Ydb::DataStreams::V1::StreamMode::ON_DEMAND:
                groupConfig.MutablePQTabletConfig()->SetMeteringMode(NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS);
                break;
            default:
                return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, static_cast<size_t>(NYds::EErrorCodes::INVALID_ARGUMENT),
                              "streams can't be created with unknown metering mode", ctx);
        }
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
        if (!GetProtoRequest()->has_partitioning_settings()) {
            if (!ValidateShardsCount(*GetProtoRequest(), pqGroupDescription, error))
            {
                return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, static_cast<size_t>(NYds::EErrorCodes::BAD_REQUEST), error, ctx);
            }

            groupConfig.SetTotalGroupCount(GetProtoRequest()->target_shard_count());
        }
        switch (GetProtoRequest()->retention_case()) {
            case Ydb::DataStreams::V1::UpdateStreamRequest::RetentionCase::kRetentionPeriodHours:
                groupConfig.MutablePQTabletConfig()->MutablePartitionConfig()->SetLifetimeSeconds(
                    TDuration::Hours(GetProtoRequest()->retention_period_hours()).Seconds());
                groupConfig.MutablePQTabletConfig()->MutablePartitionConfig()->ClearStorageLimitBytes();

                break;
            case Ydb::DataStreams::V1::UpdateStreamRequest::RetentionCase::kRetentionStorageMegabytes:
                groupConfig.MutablePQTabletConfig()->MutablePartitionConfig()->SetLifetimeSeconds(
                    TDuration::Hours(DEFAULT_STREAM_WEEK_RETENTION).Seconds());
                groupConfig.MutablePQTabletConfig()->MutablePartitionConfig()->SetStorageLimitBytes(
                    GetProtoRequest()->retention_storage_megabytes() * 1_MB);
                break;
            default: {}
        }

        auto* pqConfig = groupConfig.MutablePQTabletConfig();

        pqConfig->MutablePartitionConfig()->SetWriteSpeedInBytesPerSecond(
                    PartitionWriteSpeedInBytesPerSec(GetProtoRequest()->write_quota_kb_per_sec()));

        if (GetProtoRequest()->has_stream_mode_details()) {
            if (!AppData(ctx)->PQConfig.GetBillingMeteringConfig().GetEnabled()) {
                return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, static_cast<size_t>(NYds::EErrorCodes::INVALID_ARGUMENT),
                      "streams can't be created with metering mode", ctx);
            }

            switch(GetProtoRequest()->stream_mode_details().stream_mode()) {
                case Ydb::DataStreams::V1::StreamMode::PROVISIONED:
                    groupConfig.MutablePQTabletConfig()->SetMeteringMode(NKikimrPQ::TPQTabletConfig::METERING_MODE_RESERVED_CAPACITY);
                    break;
                case Ydb::DataStreams::V1::StreamMode::ON_DEMAND:
                    groupConfig.MutablePQTabletConfig()->SetMeteringMode(NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS);
                    break;
                default:
                    return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, static_cast<size_t>(NYds::EErrorCodes::INVALID_ARGUMENT),
                                  "streams can't be created with unknown metering mode", ctx);
            }
        }

        if (GetProtoRequest()->has_partitioning_settings()) {
            auto r = ValidatePartitioningSettings(GetProtoRequest()->partitioning_settings());
            if (!r.empty()) {
                return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, static_cast<size_t>(NYds::EErrorCodes::INVALID_ARGUMENT), r, ctx);
            }

            auto& s = GetProtoRequest()->partitioning_settings();
            auto* t = groupConfig.MutablePQTabletConfig()->MutablePartitionStrategy();

            auto& as = s.auto_partitioning_settings();
            switch(as.strategy()) {
                case Ydb::DataStreams::V1::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_UNSPECIFIED:
                case Ydb::DataStreams::V1::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_DISABLED:
                case Ydb::DataStreams::V1::AutoPartitioningStrategy::AutoPartitioningStrategy_INT_MAX_SENTINEL_DO_NOT_USE_:
                case Ydb::DataStreams::V1::AutoPartitioningStrategy::AutoPartitioningStrategy_INT_MIN_SENTINEL_DO_NOT_USE_:
                    t->SetPartitionStrategyType(NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_DISABLED);
                    break;

                case  Ydb::DataStreams::V1::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_SCALE_UP:
                    t->SetPartitionStrategyType(NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT);
                    break;

                case  Ydb::DataStreams::V1::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_SCALE_UP_AND_DOWN:
                    t->SetPartitionStrategyType(NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT_AND_MERGE);
                    break;

                case  Ydb::DataStreams::V1::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_PAUSED:
                    t->SetPartitionStrategyType(NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_PAUSED);
                    break;
            }

            t->SetMinPartitionCount(s.min_active_partitions() ? s.min_active_partitions() : 1);
            if (!s.max_active_partitions() && t->GetPartitionStrategyType() != NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_DISABLED) {
                t->SetMaxPartitionCount(1);
            } else {
                t->SetMaxPartitionCount(s.max_active_partitions());
            }

            auto& ws = as.partition_write_speed();
            t->SetScaleThresholdSeconds(ws.stabilization_window().seconds() ? ws.stabilization_window().seconds() : 300);
            t->SetScaleUpPartitionWriteSpeedThresholdPercent(ws.up_utilization_percent() ? ws.up_utilization_percent() : 90);
            t->SetScaleDownPartitionWriteSpeedThresholdPercent(ws.down_utilization_percent() ? ws.down_utilization_percent() : 30);
        }

        auto serviceTypes = GetSupportedClientServiceTypes(ctx);
        auto status = CheckConfig(*pqConfig, serviceTypes, error, ctx, Ydb::StatusIds::ALREADY_EXISTS);
        if (status != Ydb::StatusIds::SUCCESS) {
            return ReplyWithError(status, status == Ydb::StatusIds::ALREADY_EXISTS ? static_cast<size_t>(NYds::EErrorCodes::IN_USE) :
                                                                                    static_cast<size_t>(NYds::EErrorCodes::VALIDATION_ERROR),
                                                                                    error, ctx);
        }
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

        auto* pqConfig = groupConfig.MutablePQTabletConfig();

        pqConfig->MutablePartitionConfig()->SetWriteSpeedInBytesPerSecond(GetProtoRequest()->write_quota_kb_per_sec() * 1_KB);
        pqConfig->MutablePartitionConfig()->SetBurstSize(GetProtoRequest()->write_quota_kb_per_sec() * 1_KB);

        auto serviceTypes = GetSupportedClientServiceTypes(ctx);
        auto status = CheckConfig(*pqConfig, serviceTypes, error, ctx, Ydb::StatusIds::ALREADY_EXISTS);
        if (status != Ydb::StatusIds::SUCCESS) {
            return ReplyWithError(status, status == Ydb::StatusIds::ALREADY_EXISTS? static_cast<size_t>(NYds::EErrorCodes::IN_USE) :
                                                                                    static_cast<size_t>(NYds::EErrorCodes::VALIDATION_ERROR),
                                                                                    error, ctx);
        }

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
            Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS;

            auto* pqConfig = groupConfig.MutablePQTabletConfig();

            ui32 currentLifetime = pqConfig->GetPartitionConfig().GetLifetimeSeconds();
            ui32 newLifetime = TInstant::Hours(this->GetProtoRequest()->retention_period_hours()).Seconds();
            if (ShouldIncrease) {
                if (newLifetime <= currentLifetime) {
                    error = TStringBuilder() << "Retention period is not greater than provided: "
                                                 << currentLifetime << " <= " << newLifetime;
                    status = Ydb::StatusIds::BAD_REQUEST;
                }
            } else {
                if (newLifetime >= currentLifetime) {
                    error = TStringBuilder() << "Retention period is not less than provided: "
                                                 << currentLifetime << " <= " << newLifetime;
                    status = Ydb::StatusIds::BAD_REQUEST;
                }
            }
            if (status == Ydb::StatusIds::SUCCESS) {
                pqConfig->MutablePartitionConfig()->SetLifetimeSeconds(newLifetime);

                auto serviceTypes = GetSupportedClientServiceTypes(ctx);
                status = CheckConfig(*pqConfig, serviceTypes, error, ctx, Ydb::StatusIds::ALREADY_EXISTS);
            }

            if (status != Ydb::StatusIds::SUCCESS) {
                return TBase::ReplyWithError(status, status == Ydb::StatusIds::ALREADY_EXISTS ? static_cast<size_t>(NYds::EErrorCodes::IN_USE) :
                                                                                                static_cast<size_t>(NYds::EErrorCodes::VALIDATION_ERROR), error, ctx);
            }
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

        void StateWork(TAutoPtr<IEventHandle>& ev);
        void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);

        void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
            if (ev->Get()->Status != NKikimrProto::EReplyStatus::OK) {
                ReplyWithError(Ydb::StatusIds::UNAVAILABLE, Ydb::PersQueue::ErrorCode::TABLET_PIPE_DISCONNECTED,
                                          TStringBuilder() << "Cannot connect to tablet " << ev->Get()->TabletId, ctx);
            }
        }

        void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
            ReplyWithError(Ydb::StatusIds::UNAVAILABLE, Ydb::PersQueue::ErrorCode::TABLET_PIPE_DISCONNECTED,
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

    void TDescribeStreamActor::StateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPersQueue::TEvOffsetsResponse, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            default: TBase::StateWork(ev);
        }
    }

    void TDescribeStreamActor::HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const NSchemeCache::TSchemeCacheNavigate* result = ev->Get()->Request.Get();
        Y_ABORT_UNLESS(result->ResultSet.size() == 1); // describe only one topic
        const auto& response = result->ResultSet.front();
        const TString path = JoinSeq("/", response.Path);

        if (ReplyIfNotTopic(ev)) {
            return;
        }

        Y_ABORT_UNLESS(response.PQGroupInfo);

        PQGroup = response.PQGroupInfo->Description;
        SelfInfo = response.Self->Info;
        std::set<ui64> tabletIds;
        for (auto& partition : PQGroup.GetPartitions()) {
            tabletIds.insert(partition.GetTabletId());
        }
        if (tabletIds.size() == 0) {
            return ReplyAndDie(ActorContext());
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
            Pipes.push_back(ActorContext().Register(NTabletPipe::CreateClient(ActorContext().SelfID, tabletId, clientConfig)));
            TAutoPtr<TEvPersQueue::TEvOffsets> req(new TEvPersQueue::TEvOffsets);
            NTabletPipe::SendData(ActorContext(), Pipes.back(), req.Release());
        }
    }

    void TDescribeStreamActor::ReplyAndDie(const TActorContext& ctx) {
        Ydb::DataStreams::V1::DescribeStreamResult result;

        auto& pqConfig = PQGroup.GetPQTabletConfig();
        ui32 writeSpeed = pqConfig.GetPartitionConfig().GetWriteSpeedInBytesPerSecond() / 1_KB;
        auto& description = *result.mutable_stream_description();
        description.set_stream_name(GetProtoRequest()->stream_name());
        description.set_stream_arn(GetProtoRequest()->stream_name());
        ui32 retentionPeriodHours = TInstant::Seconds(pqConfig.GetPartitionConfig().GetLifetimeSeconds()).Hours();
        description.set_retention_period_hours(retentionPeriodHours);
        description.set_write_quota_kb_per_sec(writeSpeed);
        if (pqConfig.GetPartitionConfig().HasStorageLimitBytes()) {
            ui32 storageLimitMb = pqConfig.GetPartitionConfig().GetStorageLimitBytes() / 1_MB;
            description.set_storage_limit_mb(storageLimitMb);
        }
        if (SelfInfo.GetCreateFinished()) {
            description.set_stream_status(Ydb::DataStreams::V1::StreamDescription::ACTIVE);
        } else {
            description.set_stream_status(Ydb::DataStreams::V1::StreamDescription::CREATING);
        }

        if (AppData(ctx)->PQConfig.GetBillingMeteringConfig().GetEnabled()) {
            description.mutable_stream_mode_details()->set_stream_mode(
                pqConfig.GetMeteringMode() == NKikimrPQ::TPQTabletConfig::METERING_MODE_RESERVED_CAPACITY ? Ydb::DataStreams::V1::StreamMode::PROVISIONED
                                                                                                          : Ydb::DataStreams::V1::StreamMode::ON_DEMAND
                );
        }

        auto& ps = pqConfig.GetPartitionStrategy();
        auto* pt = description.mutable_partitioning_settings();
        if (ps.GetPartitionStrategyType() != NKikimrPQ::TPQTabletConfig::TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_DISABLED) {
            pt->set_min_active_partitions(ps.GetMinPartitionCount());
            pt->set_max_active_partitions(ps.GetMaxPartitionCount());

            if (ps.GetPartitionStrategyType() == NKikimrPQ::TPQTabletConfig::TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT) {
                pt->mutable_auto_partitioning_settings()->set_strategy(::Ydb::DataStreams::V1::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_SCALE_UP);
            } else if (ps.GetPartitionStrategyType() == NKikimrPQ::TPQTabletConfig::TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT_AND_MERGE) {
                pt->mutable_auto_partitioning_settings()->set_strategy(::Ydb::DataStreams::V1::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_SCALE_UP_AND_DOWN);
            } else {
                pt->mutable_auto_partitioning_settings()->set_strategy(::Ydb::DataStreams::V1::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_PAUSED);
            }

            pt->mutable_auto_partitioning_settings()->mutable_partition_write_speed()->mutable_stabilization_window()->set_seconds(ps.GetScaleThresholdSeconds());
            pt->mutable_auto_partitioning_settings()->mutable_partition_write_speed()->set_up_utilization_percent(ps.GetScaleUpPartitionWriteSpeedThresholdPercent());
            pt->mutable_auto_partitioning_settings()->mutable_partition_write_speed()->set_down_utilization_percent(ps.GetScaleDownPartitionWriteSpeedThresholdPercent());
        } else {
            pt->set_min_active_partitions(PQGroup.GetPartitions().size());
            pt->set_max_active_partitions(PQGroup.GetPartitions().size());
            pt->mutable_auto_partitioning_settings()->set_strategy(::Ydb::DataStreams::V1::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_DISABLED);
            pt->mutable_auto_partitioning_settings()->mutable_partition_write_speed()->mutable_stabilization_window()->set_seconds(300);
            pt->mutable_auto_partitioning_settings()->mutable_partition_write_speed()->set_up_utilization_percent(90);
            pt->mutable_auto_partitioning_settings()->mutable_partition_write_speed()->set_down_utilization_percent(30);
        }

        bool startShardFound = GetProtoRequest()->exclusive_start_shard_id().empty();
        description.set_has_more_shards(false);

        description.set_owner(SelfInfo.GetOwner());
        description.set_stream_creation_timestamp(SelfInfo.GetCreateStep());

        int limit = GetProtoRequest()->limit() == 0 ? 100 : GetProtoRequest()->limit();

        for (uint32_t i = 0; i < (uint32_t)PQGroup.GetPartitions().size(); ++i) {
            auto partition = PQGroup.GetPartitions(i);
            ui32 partitionId = partition.GetPartitionId();
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

                    const auto& parents = partition.GetParentPartitionIds();
                    if (parents.size() > 0) {
                        shard->set_parent_shard_id(GetShardName(parents[0]));
                    }
                    if (parents.size() > 1) {
                        shard->set_adjacent_parent_shard_id(GetShardName(parents[1]));
                    }

                    auto* rangeProto = shard->mutable_hash_key_range();
                    if (NPQ::SplitMergeEnabled(pqConfig)) {
                        NYql::NDecimal::TUint128 from = partition.HasKeyRange() && partition.GetKeyRange().HasFromBound() ? NPQ::AsInt<NYql::NDecimal::TUint128>(partition.GetKeyRange().GetFromBound()) + 1: 0;
                        NYql::NDecimal::TUint128 to = partition.HasKeyRange() && partition.GetKeyRange().HasToBound() ? NPQ::AsInt<NYql::NDecimal::TUint128>(partition.GetKeyRange().GetToBound()): -1;
                        rangeProto->set_starting_hash_key(Uint128ToDecimalString(from));
                        rangeProto->set_ending_hash_key(Uint128ToDecimalString(to));
                    } else {
                        auto range = RangeFromShardNumber(partitionId, PQGroup.GetPartitions().size());
                        rangeProto->set_starting_hash_key(Uint128ToDecimalString(range.Start));
                        rangeProto->set_ending_hash_key(Uint128ToDecimalString(range.End));
                    }
                    auto it = StartEndOffsetsPerPartition.find(partitionId);
                    if (it != StartEndOffsetsPerPartition.end()) {
                        auto* rangeProto = shard->mutable_sequence_number_range();
                        rangeProto->set_starting_sequence_number(TStringBuilder() << it->second.first);
                    }
                }
            }
        }
        if (!startShardFound) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::BAD_REQUEST,
                                  TStringBuilder() << "Bad shard id " << GetProtoRequest()->exclusive_start_shard_id(), ctx);
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

        void StateWork(TAutoPtr<IEventHandle>& ev);
        void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx);

    protected:
        void SendNavigateRequest(const TActorContext& ctx, const TString& path);
        void SendPendingRequests(const TActorContext& ctx);
        void SendResponse(const TActorContext& ctx);

        void ReplyWithError(Ydb::StatusIds::StatusCode status, NYds::EErrorCodes errorCode,
                            const TString& messageText, const NActors::TActorContext& ctx) {

            this->Request_->RaiseIssue(FillIssue(messageText, static_cast<size_t>(errorCode)));
            this->Request_->ReplyWithYdbStatus(status);
            this->Die(ctx);
        }

    private:
        static constexpr ui32 MAX_IN_FLIGHT = 100;

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
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, NYds::EErrorCodes::INVALID_ARGUMENT,
                                          "Request without dabase is forbiden", ctx);
        }

        if (this->Request_->GetSerializedToken().empty()) {
            if (AppData(ctx)->PQConfig.GetRequireCredentialsInNewProtocol()) {
                return ReplyWithError(Ydb::StatusIds::UNAUTHORIZED, NYds::EErrorCodes::BAD_REQUEST,
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
            Request_->RaiseIssue(FillIssue("'Limit' shoud not be higher than 10000", static_cast<size_t>(NYds::EErrorCodes::VALIDATION_ERROR)));
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

        if (!this->Request_->GetSerializedToken().empty()) {
            schemeCacheRequest->UserToken = new NACLib::TUserToken(this->Request_->GetSerializedToken());
        }

        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;
        schemeCacheRequest->ResultSet.emplace_back(entry);
        WaitingList.push_back(std::make_unique<TEvTxProxySchemeCache::TEvNavigateKeySet>(schemeCacheRequest.release()));
        SendPendingRequests(ctx);
    }

    void TListStreamsActor::StateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            default: TBase::StateWork(ev);
        }
    }

    void TListStreamsActor::Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
        const NSchemeCache::TSchemeCacheNavigate* navigate = ev->Get()->Request.Get();
        for (const auto& entry : navigate->ResultSet) {


            if (entry.Kind == NSchemeCache::TSchemeCacheNavigate::EKind::KindTable) {
                for (const auto& stream : entry.CdcStreams) {
                    TString childFullPath = JoinPath({JoinPath(entry.Path), stream.GetName()});
                    Topics.push_back(childFullPath);
                }
            } else if (entry.Kind == NSchemeCache::TSchemeCacheNavigate::EKind::KindPath
                || entry.Kind == NSchemeCache::TSchemeCacheNavigate::EKind::KindSubdomain)
            {
                Y_ENSURE(entry.ListNodeEntry, "ListNodeEntry is zero");
                for (const auto& child : entry.ListNodeEntry->Children) {
                    TString childFullPath = JoinPath({JoinPath(entry.Path), child.Name});
                    switch (child.Kind) {
                        case NSchemeCache::TSchemeCacheNavigate::EKind::KindPath:
                        case NSchemeCache::TSchemeCacheNavigate::EKind::KindTable:
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
        void StateWork(TAutoPtr<IEventHandle>& ev);
        void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);

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

        if (!GetProtoRequest()->next_token().empty() && !GetProtoRequest()->stream_arn().empty()) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, static_cast<size_t>(NYds::EErrorCodes::INVALID_PARAMETER_COMBINATION),
                                  TStringBuilder() << "StreamArn and NextToken can not be provided together", ctx);
        }
        if (NextToken.IsExpired()) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, static_cast<size_t>(NYds::EErrorCodes::EXPIRED_TOKEN),
                                  TStringBuilder() << "Provided NextToken is expired", ctx);
        }
        if (!NextToken.IsValid()) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, static_cast<size_t>(NYds::EErrorCodes::INVALID_ARGUMENT),
                                  TStringBuilder() << "Provided NextToken is malformed", ctx);
        }

        auto maxResultsInRange = MIN_MAX_RESULTS <= MaxResults && MaxResults <= MAX_MAX_RESULTS;
        if (!maxResultsInRange) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, static_cast<size_t>(NYds::EErrorCodes::VALIDATION_ERROR),
                                  TStringBuilder() << "Requested max_result value '" << MaxResults <<
                                  "' is out of range [" << MIN_MAX_RESULTS << ", " << MAX_MAX_RESULTS <<
                                  "]", ctx);
        }
        SendDescribeProposeRequest(ctx);
        Become(&TListStreamConsumersActor::StateWork);
    }

    void TListStreamConsumersActor::StateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            default: TBase::StateWork(ev);
        }
    }

    void TListStreamConsumersActor::HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const NSchemeCache::TSchemeCacheNavigate* result = ev->Get()->Request.Get();
        Y_ABORT_UNLESS(result->ResultSet.size() == 1); // describe only one topic

        if (ReplyIfNotTopic(ev)) {
            return;
        }

        std::vector<std::pair<TString, ui64>> readRules;
        ui32 leftToRead{0};
        const auto& response = result->ResultSet.front();
        const auto& pqGroupDescription = response.PQGroupInfo->Description;
        const auto& streamConsumers = pqGroupDescription.GetPQTabletConfig().GetConsumers();
        const auto alreadyRead = NextToken.GetAlreadyRead();

        ui32 consumerCount = NPQ::ConsumerCount(pqGroupDescription.GetPQTabletConfig());

        if (alreadyRead > consumerCount) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, static_cast<size_t>(NYds::EErrorCodes::INVALID_ARGUMENT),
                                  TStringBuilder() << "Provided next_token is malformed - " <<
                                  "everything is already read", ActorContext());
        }

        const auto rulesToRead = std::min(consumerCount - alreadyRead, MaxResults);
        readRules.reserve(rulesToRead);

        auto consumer = streamConsumers.begin() + alreadyRead;
        for (auto i = rulesToRead; i > 0; --i, ++consumer) {
            readRules.push_back({consumer->GetName(), consumer->GetReadFromTimestampsMs()});
        }
        leftToRead = consumerCount - alreadyRead - rulesToRead;

        SendResponse(ActorContext(), readRules, leftToRead);
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
        void OnNotifyTxCompletionResult(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev, const TActorContext& ctx) override;

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

        auto messageAndCode = AddReadRuleToConfig(pqConfig, readRule, serviceTypes, ctx);
        size_t issueCode = static_cast<size_t>(messageAndCode.PQCode);

        Ydb::StatusIds::StatusCode status;
        if (messageAndCode.PQCode == Ydb::PersQueue::ErrorCode::OK) {
            status = CheckConfig(*pqConfig, serviceTypes, messageAndCode.Message, ctx, Ydb::StatusIds::ALREADY_EXISTS);
            if (status == Ydb::StatusIds::ALREADY_EXISTS) {
                issueCode = static_cast<size_t>(NYds::EErrorCodes::IN_USE);
            }
        } else {
            status = Ydb::StatusIds::BAD_REQUEST;
        }

        if (status != Ydb::StatusIds::SUCCESS) {
            return ReplyWithError(status, issueCode, messageAndCode.Message, ctx);
        }
    }

    void TRegisterStreamConsumerActor::OnNotifyTxCompletionResult(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev, const TActorContext& ctx) {
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
            return ReplyWithError(Ydb::StatusIds::NOT_FOUND, static_cast<size_t>(NYds::EErrorCodes::NOT_FOUND), error, ctx);
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
        void StateWork(TAutoPtr<IEventHandle>& ev);
        void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);


    private:
        using TIteratorType = Ydb::DataStreams::V1::ShardIteratorType;

        void SendResponse(const TActorContext& ctx, const TShardIterator& shardIt);
        std::optional<ui64> SequenceNumberToInt(const TString& sequenceNumberStr);

        TString StreamName;
        TString ShardId;
        TIteratorType IteratorType;
        ui64 SequenceNumber;
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
                return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, static_cast<size_t>(NYds::EErrorCodes::INVALID_ARGUMENT),
                                      TStringBuilder() << "Malformed sequence number", ctx);
            }
            SequenceNumber = sn.value() + (IteratorType == TIteratorType::AFTER_SEQUENCE_NUMBER ? 1u : 0u);
            }
            break;
        case TIteratorType::AT_TIMESTAMP:
            if (GetProtoRequest()->timestamp() == 0) {
                return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, static_cast<size_t>(NYds::EErrorCodes::INVALID_ARGUMENT),
                                      TStringBuilder() << "Shard iterator type is AT_TIMESTAMP, " <<
                                      "but a timestamp is missed", ctx);

            }
            if (GetProtoRequest()->timestamp() > static_cast<i64>(TInstant::Now().MilliSeconds()) + TIMESTAMP_DELTA_ALLOWED_MS) {
                return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, static_cast<size_t>(NYds::EErrorCodes::INVALID_ARGUMENT),
                                      TStringBuilder() << "Shard iterator type is AT_TIMESTAMP, " <<
                                      "but a timestamp is in the future", ctx);
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
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, static_cast<size_t>(NYds::EErrorCodes::INVALID_ARGUMENT),
                                  TStringBuilder() << "Shard iterator type '" <<
                                  (ui32)IteratorType << "' is not known", ctx);

        }

        SendDescribeProposeRequest(ctx);
        Become(&TGetShardIteratorActor::StateWork);
    }

    void TGetShardIteratorActor::StateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
        default: TBase::StateWork(ev);
        }
    }

    void TGetShardIteratorActor::HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        if (ReplyIfNotTopic(ev)) {
            return;
        }

        const NSchemeCache::TSchemeCacheNavigate* navigate = ev->Get()->Request.Get();
        auto topicInfo = navigate->ResultSet.begin();
        StreamName = NKikimr::CanonizePath(topicInfo->Path);
        if (AppData(ActorContext())->PQConfig.GetRequireCredentialsInNewProtocol()) {
            NACLib::TUserToken token(this->Request_->GetSerializedToken());

            if (!topicInfo->SecurityObject->CheckAccess(NACLib::EAccessRights::SelectRow,
                                                        token)) {
                return this->ReplyWithError(Ydb::StatusIds::UNAUTHORIZED,
                                            static_cast<size_t>(NYds::EErrorCodes::ACCESS_DENIED),
                                            TStringBuilder() << "Access to stream "
                                            << this->GetProtoRequest()->stream_name()
                                            << " is denied for subject "
                                            << token.GetUserSID(), ActorContext());
            }
        }

        const auto& partitions = topicInfo->PQGroupInfo->Description.GetPartitions();
        for (auto& partition : partitions) {
            auto partitionId = partition.GetPartitionId();
            TString shardName = GetShardName(partitionId);
            if (shardName == ShardId) {
                if (topicInfo->ShowPrivatePath) {
                    SendResponse(ActorContext(),
                                 TShardIterator::Cdc(StreamName, StreamName, partitionId, ReadTimestampMs, SequenceNumber));
                } else {
                    SendResponse(ActorContext(),
                                 TShardIterator::Common(StreamName, StreamName, partitionId, ReadTimestampMs, SequenceNumber));
                }
                return;
            }
        }

        ReplyWithError(Ydb::StatusIds::BAD_REQUEST, static_cast<size_t>(NYds::EErrorCodes::NOT_FOUND),
                       TStringBuilder() << "No such shard: " << ShardId, ActorContext());
    }

    void TGetShardIteratorActor::SendResponse(const TActorContext& ctx, const TShardIterator& shardIt) {
        Ydb::DataStreams::V1::GetShardIteratorResult result;
        result.set_shard_iterator(shardIt.Serialize());
        Request_->SendResult(result, Ydb::StatusIds::SUCCESS);
        Die(ctx);
    }

    std::optional<ui64> TGetShardIteratorActor::SequenceNumberToInt(const TString& sequenceNumberStr) {
        try {
            return std::stoull(sequenceNumberStr.c_str());
        } catch(...) {
            return std::nullopt;
        }
    }

    //-----------------------------------------------------------------------------------

    class TGetRecordsActor : public TPQGrpcSchemaBase<TGetRecordsActor, TEvDataStreamsGetRecordsRequest>
                           , private NPQ::TRlHelpers
                           , public TCdcStreamCompatible
    {
        using TBase = TPQGrpcSchemaBase<TGetRecordsActor, TEvDataStreamsGetRecordsRequest>;
        using TProtoRequest = typename TBase::TProtoRequest;
        using EWakeupTag = TRlHelpers::EWakeupTag;

        static constexpr ui32 READ_TIMEOUT_MS = 150;
        static constexpr i32 MAX_LIMIT = 10000;

    public:
        TGetRecordsActor(NKikimr::NGRpcService::IRequestOpCtx* request);
        ~TGetRecordsActor() = default;

        void Bootstrap(const NActors::TActorContext& ctx);
        void StateWork(TAutoPtr<IEventHandle>& ev);
        void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
        void Handle(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx);
        void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx);
        void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx);
        void Handle(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx);
        void Die(const TActorContext& ctx) override;

    private:
        void SendReadRequest(const TActorContext& ctx);
        void SendResponse(const TActorContext& ctx);
        ui64 GetPayloadSize() const;

        TShardIterator ShardIterator;
        TString StreamName;
        ui64 TabletId;
        i32 Limit;
        TActorId PipeClient;
        Ydb::DataStreams::V1::GetRecordsResult Result;
    };

    TGetRecordsActor::TGetRecordsActor(NKikimr::NGRpcService::IRequestOpCtx* request)
        : TBase(request, TShardIterator(GetRequest<TProtoRequest>(request)->shard_iterator()).IsValid()
                ? TShardIterator(GetRequest<TProtoRequest>(request)->shard_iterator()).GetStreamName()
                : "undefined")
        , TRlHelpers({}, request, 8_KB, false, TDuration::Seconds(1))
        , ShardIterator{GetRequest<TProtoRequest>(request)->shard_iterator()}
        , StreamName{ShardIterator.IsValid() ? ShardIterator.GetStreamName() : "undefined"}
        , TabletId{0}
        , Limit{GetRequest<TProtoRequest>(request)->limit()}
    {
    }

    void TGetRecordsActor::Bootstrap(const NActors::TActorContext& ctx) {
        TBase::Bootstrap(ctx);

        if (ShardIterator.IsExpired()) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, static_cast<size_t>(NYds::EErrorCodes::EXPIRED_ITERATOR),
                                  TStringBuilder() << "Provided shard iterator is expired", ctx);
        }
        if (!ShardIterator.IsValid()) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, static_cast<size_t>(NYds::EErrorCodes::INVALID_ARGUMENT),
                                  TStringBuilder() << "Provided shard iterator is malformed", ctx);
        }

        Limit = Limit == 0 ? MAX_LIMIT : Limit;
        if (Limit < 1 || Limit > MAX_LIMIT) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, static_cast<size_t>(NYds::EErrorCodes::VALIDATION_ERROR),
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
        request.MutablePartitionRequest()->SetTopic(this->GetTopicPath());
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

    void TGetRecordsActor::StateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPersQueue::TEvResponse, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvents::TEvWakeup, Handle);
        default: TBase::StateWork(ev);
        }
    }

    void TGetRecordsActor::HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto &result = ev->Get()->Request.Get();
        const auto response = result->ResultSet.front();

        if (AppData(ActorContext())->PQConfig.GetRequireCredentialsInNewProtocol()) {
            NACLib::TUserToken token(this->Request_->GetSerializedToken());

            if (!response.SecurityObject->CheckAccess(NACLib::EAccessRights::SelectRow,
                                                      token)) {
                return ReplyWithError(Ydb::StatusIds::UNAUTHORIZED,
                                      static_cast<size_t>(NYds::EErrorCodes::ACCESS_DENIED),
                                      TStringBuilder() << "Access to stream "
                                      << ShardIterator.GetStreamName()
                                      << " is denied for subject "
                                      << token.GetUserSID(), ActorContext());
            }
        }

        if (response.Self->Info.GetPathType() == NKikimrSchemeOp::EPathTypePersQueueGroup) {
            SetMeteringMode(response.PQGroupInfo->Description.GetPQTabletConfig().GetMeteringMode());

            const auto& partitions = response.PQGroupInfo->Description.GetPartitions();
            for (auto& partition : partitions) {
                auto partitionId = partition.GetPartitionId();
                if (partitionId == ShardIterator.GetShardId()) {
                    TabletId = partition.GetTabletId();
                    return SendReadRequest(ActorContext());
                }
            }
        }

        ReplyWithError(Ydb::StatusIds::BAD_REQUEST, static_cast<size_t>(NYds::EErrorCodes::NOT_FOUND),
                       TStringBuilder() << "No such shard: " << ShardIterator.GetShardId(), ActorContext());
    }

    void TGetRecordsActor::Handle(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;
        switch (record.GetStatus()) {
            case NMsgBusProxy::MSTATUS_ERROR:
                switch (record.GetErrorCode()) {
                    case NPersQueue::NErrorCode::READ_ERROR_TOO_SMALL_OFFSET:
                    case NPersQueue::NErrorCode::READ_ERROR_TOO_BIG_OFFSET:
                        Result.set_next_shard_iterator(TShardIterator(ShardIterator).Serialize());
                        Result.set_millis_behind_latest(0);

                        if (IsQuotaRequired()) {
                            Y_ABORT_UNLESS(MaybeRequestQuota(1, EWakeupTag::RlAllowed, ctx));
                        } else {
                            SendResponse(ctx);
                        }
                        return;
                    default:
                        return ReplyWithError(ConvertPersQueueInternalCodeToStatus(record.GetErrorCode()),
                                              ConvertOldCode(record.GetErrorCode()),
                                              record.GetErrorReason(), ctx);
                }
                break;
            default: {}
        }

        TShardIterator shardIterator(ShardIterator);
        const auto& response = record.GetPartitionResponse();
        if (response.HasCmdReadResult()) {
            const auto& results = response.GetCmdReadResult().GetResult();
            for (auto& r : results) {
                auto proto(NKikimr::GetDeserializedData(r.GetData()));
                auto record = Result.add_records();
                record->set_data(proto.GetData());
                record->set_encryption_type(Ydb::DataStreams::V1::EncryptionType::NONE);
                record->set_approximate_arrival_timestamp(r.GetCreateTimestampMS());
                record->set_partition_key(r.GetPartitionKey());
                record->set_sequence_number(std::to_string(r.GetOffset()).c_str());
                if (proto.GetCodec() > 0) {
                    record->set_codec(proto.GetCodec() + 1);
                }
            }
            if (!results.empty()) {
                auto last = results.rbegin();
                shardIterator.SetReadTimestamp(0);
                shardIterator.SetSequenceNumber(last->GetOffset() + 1);
                Result.set_millis_behind_latest(TInstant::Now().MilliSeconds() - last->GetWriteTimestampMS());
            } else { // remove else?
                Result.set_millis_behind_latest(0);
            }
        }

        Result.set_next_shard_iterator(shardIterator.Serialize());

        if (IsQuotaRequired()) {
            const auto ru = 1 + CalcRuConsumption(GetPayloadSize());
            Y_ABORT_UNLESS(MaybeRequestQuota(ru, EWakeupTag::RlAllowed, ctx));
        } else {
            SendResponse(ctx);
        }
    }

    void TGetRecordsActor::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
        if (ev->Get()->Status != NKikimrProto::EReplyStatus::OK) {
            ReplyWithError(Ydb::StatusIds::UNAVAILABLE, Ydb::PersQueue::ErrorCode::TABLET_PIPE_DISCONNECTED,
                           TStringBuilder() << "Cannot connect to tablet " << ev->Get()->TabletId, ctx);
        }
    }

    void TGetRecordsActor::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
        ReplyWithError(Ydb::StatusIds::UNAVAILABLE, Ydb::PersQueue::ErrorCode::TABLET_PIPE_DISCONNECTED,
                       TStringBuilder() << "Cannot connect to tablet " << ev->Get()->TabletId, ctx);
    }

    void TGetRecordsActor::Handle(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx) {
        switch (static_cast<EWakeupTag>(ev->Get()->Tag)) {
            case EWakeupTag::RlAllowed:
                return SendResponse(ctx);
            case EWakeupTag::RlNoResource:
                return RespondWithCode(Ydb::StatusIds::OVERLOADED);
            default:
                return HandleWakeup(ev, ctx);
        }
    }

    void TGetRecordsActor::SendResponse(const TActorContext& ctx) {
        Request_->SendResult(Result, Ydb::StatusIds::SUCCESS);
        Die(ctx);
    }

    ui64 TGetRecordsActor::GetPayloadSize() const {
        ui64 result = 0;

        for (const auto& record : Result.records()) {
            result += record.data().size()
                + record.partition_key().size();
        }

        return result;
    }

    void TGetRecordsActor::Die(const TActorContext& ctx) {
        NTabletPipe::CloseClient(ctx, PipeClient);
        TRlHelpers::PassAway(SelfId());
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
        void StateWork(TAutoPtr<IEventHandle>& ev);
        void Handle(TEvPersQueue::TEvOffsetsResponse::TPtr& ev, const TActorContext& ctx);
        void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx);
        void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx);
        void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
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

        if (!GetProtoRequest()->next_token().empty() && !GetProtoRequest()->stream_name().empty()) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, static_cast<size_t>(NYds::EErrorCodes::INVALID_PARAMETER_COMBINATION),
                                  TStringBuilder() << "StreamName and NextToken can not be provided together", ctx);
        }
        if (NextToken.IsExpired()) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, static_cast<size_t>(NYds::EErrorCodes::EXPIRED_TOKEN),
                                  TStringBuilder() << "Provided next token is expired", ctx);
        }
        if (!NextToken.IsValid()) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, static_cast<size_t>(NYds::EErrorCodes::INVALID_ARGUMENT),
                                  TStringBuilder() << "Provided next token is malformed", ctx);
        }

        if (!TShardFilter::ShardFilterType_IsValid(ShardFilter.type())) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, static_cast<size_t>(NYds::EErrorCodes::INVALID_ARGUMENT),
                                  TStringBuilder() << "Shard filter '" <<
                                  (ui32)ShardFilter.type() << "' is not known", ctx);
        }

        MaxResults = MaxResults == 0 ? DEFAULT_MAX_RESULTS : MaxResults;
        if (MaxResults > MAX_MAX_RESULTS) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, static_cast<size_t>(NYds::EErrorCodes::VALIDATION_ERROR),
                                  TStringBuilder() << "Max results '" << MaxResults <<
                                  "' is out of bound [" << MIN_MAX_RESULTS << "; " <<
                                  MAX_MAX_RESULTS << "]", ctx);
        }

        if (ShardFilter.type() == TShardFilter::AFTER_SHARD_ID && ShardFilter.shard_id() == "") {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, static_cast<size_t>(NYds::EErrorCodes::MISSING_PARAMETER),
                                  TStringBuilder() << "Shard filter type is AFTER_SHARD_ID," <<
                                  " but no ShardId provided", ctx);
        }

        SendDescribeProposeRequest(ctx);
        Become(&TListShardsActor::StateWork);
    }

    void TListShardsActor::StateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPersQueue::TEvOffsetsResponse, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
        default: TBase::StateWork(ev);
        }
    }

    void TListShardsActor::HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        if (ReplyIfNotTopic(ev)) {
            return;
        }
        auto ctx = ActorContext();

        const NSchemeCache::TSchemeCacheNavigate* navigate = ev->Get()->Request.Get();
        auto topicInfo = navigate->ResultSet.front();
        if (AppData(ctx)->PQConfig.GetRequireCredentialsInNewProtocol()) {
            NACLib::TUserToken token(this->Request_->GetSerializedToken());

            if (!topicInfo.SecurityObject->CheckAccess(NACLib::EAccessRights::SelectRow,
                                                        token)) {
                return this->ReplyWithError(Ydb::StatusIds::UNAUTHORIZED,
                                            static_cast<size_t>(NYds::EErrorCodes::ACCESS_DENIED),
                                            TStringBuilder() << "Access to stream "
                                            << this->GetProtoRequest()->stream_name()
                                            << " is denied for subject "
                                            << token.GetUserSID(), ActorContext());
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
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, static_cast<size_t>(NYds::EErrorCodes::INVALID_ARGUMENT),
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
            ReplyWithError(Ydb::StatusIds::UNAVAILABLE, Ydb::PersQueue::ErrorCode::TABLET_PIPE_DISCONNECTED,
                           TStringBuilder() << "Cannot connect to tablet " << ev->Get()->TabletId, ctx);
        }
    }

    void TListShardsActor::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
        ReplyWithError(Ydb::StatusIds::UNAVAILABLE, Ydb::PersQueue::ErrorCode::TABLET_PIPE_DISCONNECTED,
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
            //TODO: fill it only for closed partitions
            //awsShard->mutable_sequence_number_range()->set_ending_sequence_number(
            //    std::to_string(StartEndOffsetsPerPartition[shard.GetPartitionId()].second));
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

        void StateWork(TAutoPtr<IEventHandle>& ev);
        void HandleCacheNavigateResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);

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

    void TDescribeStreamSummaryActor::StateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
        default: TBase::StateWork(ev);
        }
    }

    void TDescribeStreamSummaryActor::HandleCacheNavigateResponse(
        TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev
    ) {
        if (ReplyIfNotTopic(ev)) {
            return;
        }

        const NSchemeCache::TSchemeCacheNavigate* result = ev->Get()->Request.Get();
        Y_ABORT_UNLESS(result->ResultSet.size() == 1); // describe only one topic
        const auto& response = result->ResultSet.front();
        Y_ABORT_UNLESS(response.PQGroupInfo);
        const TString path = JoinSeq("/", response.Path);

        PQGroup = response.PQGroupInfo->Description;
        SelfInfo = response.Self->Info;

        SendResponse(ActorContext());
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
        descriptionSummary.set_consumer_count(NPQ::ConsumerCount(PQGroup.GetPQTabletConfig()));
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
            this->Request_->RaiseIssue(FillIssue("Method is not implemented yet", static_cast<size_t>(NYds::EErrorCodes::ERROR)));
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
DECLARE_RPC(UpdateStreamMode);
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
    Y_ABORT_UNLESS(req != nullptr, "Wrong using of TGRpcRequestWrapper");
    TActivationContext::AsActorContext().Register(new TSetStreamRetentionPeriodActor<TEvDataStreamsDecreaseStreamRetentionPeriodRequest>(req, false));
}
template<>
IActor* TEvDataStreamsDecreaseStreamRetentionPeriodRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
    return new TSetStreamRetentionPeriodActor<TEvDataStreamsDecreaseStreamRetentionPeriodRequest>(dynamic_cast<TEvDataStreamsDecreaseStreamRetentionPeriodRequest*>(msg), false);
}

void DoDataStreamsIncreaseStreamRetentionPeriodRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
    auto* req = dynamic_cast<TEvDataStreamsIncreaseStreamRetentionPeriodRequest*>(p.release());
    Y_ABORT_UNLESS(req != nullptr, "Wrong using of TGRpcRequestWrapper");
    TActivationContext::AsActorContext().Register(new TSetStreamRetentionPeriodActor<TEvDataStreamsIncreaseStreamRetentionPeriodRequest>(req, true));
}
template<>
IActor* TEvDataStreamsIncreaseStreamRetentionPeriodRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
    return new TSetStreamRetentionPeriodActor<TEvDataStreamsIncreaseStreamRetentionPeriodRequest>(dynamic_cast<TEvDataStreamsIncreaseStreamRetentionPeriodRequest*>(msg), true);
}


}
