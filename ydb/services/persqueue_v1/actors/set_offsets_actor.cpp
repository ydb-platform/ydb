#include "set_offsets_actor.h"

#include <ydb/core/grpc_services/rpc_calls_topic.h>
#include <ydb/core/persqueue/public/set_offsets/set_offsets.h>
#include <ydb/services/persqueue_v1/actors/schema/common/grpc_proxy_actor.h>

#include <google/protobuf/util/time_util.h>

#include <algorithm>
#include <vector>

namespace NKikimr::NGRpcProxy::V1 {

namespace {

class TGrpcSetOffsetsActor: public TGrpcProxyActor<TGrpcSetOffsetsActor, NGRpcService::TEvSetOffsetsRequest> {
    using TRpcOpBase = NGRpcService::TRpcOperationRequestActor<TGrpcSetOffsetsActor, NGRpcService::TEvSetOffsetsRequest>;

public:
    TGrpcSetOffsetsActor(NGRpcService::IRequestOpCtx* request)
        : TGrpcProxyActor<TGrpcSetOffsetsActor, NGRpcService::TEvSetOffsetsRequest>(request)
    {
    }

    void DoAction() {
        const auto* proto = GetProtoRequest();
        if (proto->position_case() == Ydb::Topic::SetOffsetsRequest::POSITION_NOT_SET) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, "Position is required");
        }

        Become(&TGrpcSetOffsetsActor::StateWork);

        NPQ::NSetOffsets::TSetOffsetsSettings settings;
        settings.DatabasePath = GetDatabase();
        settings.TopicName = proto->path();
        settings.Consumer = proto->consumer();
        settings.UserToken = GetUserToken();

        switch (proto->position_case()) {
            case Ydb::Topic::SetOffsetsRequest::kEarliest:
                settings.Position = NKikimrPQ::TEvSetOffsetsRequest::EARLIEST;
                break;
            case Ydb::Topic::SetOffsetsRequest::kLatest:
                settings.Position = NKikimrPQ::TEvSetOffsetsRequest::LATEST;
                break;
            case Ydb::Topic::SetOffsetsRequest::kFromWrittenAt: {
                settings.Position = NKikimrPQ::TEvSetOffsetsRequest::FROM_WRITTEN_AT;
                const i64 timestampMs = ::google::protobuf::util::TimeUtil::TimestampToMilliseconds(
                    proto->from_written_at().written_at());
                settings.TimestampMs = timestampMs < 0 ? 0 : static_cast<ui64>(timestampMs);
                break;
            }
            case Ydb::Topic::SetOffsetsRequest::POSITION_NOT_SET:
                Y_ABORT("unreachable");
        }

        Register(NPQ::NSetOffsets::CreateSetOffsetsActor(SelfId(), std::move(settings)));
    }

private:
    void Handle(NPQ::NSetOffsets::TEvSetOffsetsResult::TPtr& ev) {
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            return ReplyWithError(ev->Get()->Status, ev->Get()->Error);
        }

        std::vector<const NPQ::NSetOffsets::TPartitionResult*> failedPartitions;
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
                failed << "Failed to set offsets for partitions: ";
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

        ReplyWithResult(Ydb::StatusIds::SUCCESS, Ydb::Topic::SetOffsetsResult());
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NPQ::NSetOffsets::TEvSetOffsetsResult, Handle);
            default:
                TRpcOpBase::StateFuncBase(ev);
        }
    }
};

} // namespace

NActors::IActor* CreateSetOffsetsActor(NGRpcService::IRequestOpCtx* request) {
    return new TGrpcSetOffsetsActor(request);
}

} // namespace NKikimr::NGRpcProxy::V1
