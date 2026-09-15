#include "reset_offset_actor.h"

#include <ydb/core/grpc_services/rpc_calls_topic.h>
#include <ydb/core/persqueue/public/reset_offset/reset_offset.h>
#include <ydb/services/persqueue_v1/actors/schema/common/grpc_proxy_actor.h>

#include <google/protobuf/util/time_util.h>

#include <algorithm>
#include <vector>

namespace NKikimr::NGRpcProxy::V1 {

namespace {

class TGrpcResetOffsetActor: public TGrpcProxyActor<TGrpcResetOffsetActor, NGRpcService::TEvResetOffsetRequest> {
    using TRpcOpBase = NGRpcService::TRpcOperationRequestActor<TGrpcResetOffsetActor, NGRpcService::TEvResetOffsetRequest>;

public:
    TGrpcResetOffsetActor(NGRpcService::IRequestOpCtx* request)
        : TGrpcProxyActor<TGrpcResetOffsetActor, NGRpcService::TEvResetOffsetRequest>(request)
    {
    }

    void DoAction() {
        const auto* proto = GetProtoRequest();
        if (proto->position_case() == Ydb::Topic::ResetOffsetRequest::POSITION_NOT_SET) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, "Position is required");
        }

        Become(&TGrpcResetOffsetActor::StateWork);

        NPQ::NResetOffset::TResetOffsetSettings settings;
        settings.DatabasePath = GetDatabase();
        settings.TopicName = proto->path();
        settings.Consumer = proto->consumer();
        settings.UserToken = GetUserToken();

        switch (proto->position_case()) {
            case Ydb::Topic::ResetOffsetRequest::kEarliest:
                settings.Position = NKikimrPQ::TEvResetOffsetRequest::EARLIEST;
                break;
            case Ydb::Topic::ResetOffsetRequest::kLatest:
                settings.Position = NKikimrPQ::TEvResetOffsetRequest::LATEST;
                break;
            case Ydb::Topic::ResetOffsetRequest::kFromWrittenAt: {
                settings.Position = NKikimrPQ::TEvResetOffsetRequest::FROM_WRITTEN_AT;
                const i64 timestampMs = ::google::protobuf::util::TimeUtil::TimestampToMilliseconds(
                    proto->from_written_at().written_at());
                settings.TimestampMs = timestampMs < 0 ? 0 : static_cast<ui64>(timestampMs);
                break;
            }
            case Ydb::Topic::ResetOffsetRequest::POSITION_NOT_SET:
                Y_ABORT("unreachable");
        }

        Register(NPQ::NResetOffset::CreateResetOffsetActor(SelfId(), std::move(settings)));
    }

private:
    void Handle(NPQ::NResetOffset::TEvResetOffsetResult::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            return ReplyWithError(ev->Get()->Status, ev->Get()->Error);
        }

        std::vector<const NPQ::NResetOffset::TPartitionResult*> failedPartitions;
        for (const auto& partition : ev->Get()->Partitions) {
            if (partition.Status != Ydb::StatusIds::SUCCESS) {
                failedPartitions.push_back(&partition);
            }
        }
        std::sort(failedPartitions.begin(), failedPartitions.end(), [](const auto* lhs, const auto* rhs) {
            return lhs->PartitionId < rhs->PartitionId;
        });

        TStringBuilder failed;
        Ydb::StatusIds::StatusCode errorStatus = Ydb::StatusIds::GENERIC_ERROR;
        for (size_t i = 0; i < failedPartitions.size(); ++i) {
            const auto& partition = *failedPartitions[i];
            if (i == 0) {
                failed << "Failed to reset offset for partitions: ";
                errorStatus = partition.Status;
            } else {
                failed << ", ";
            }
            failed << partition.PartitionId;
            if (!partition.Error.empty()) {
                failed << " (" << partition.Error << ")";
            }
            if (partition.Status == Ydb::StatusIds::OVERLOADED) {
                errorStatus = Ydb::StatusIds::OVERLOADED;
            }
        }

        if (!failedPartitions.empty()) {
            return ReplyWithError(errorStatus, failed);
        }

        ReplyWithResult(Ydb::StatusIds::SUCCESS, Ydb::Topic::ResetOffsetResult());
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NPQ::NResetOffset::TEvResetOffsetResult, Handle);
            default:
                TRpcOpBase::StateFuncBase(ev);
        }
    }
};

} // namespace

NActors::IActor* CreateResetOffsetActor(NGRpcService::IRequestOpCtx* request) {
    return new TGrpcResetOffsetActor(request);
}

} // namespace NKikimr::NGRpcProxy::V1
