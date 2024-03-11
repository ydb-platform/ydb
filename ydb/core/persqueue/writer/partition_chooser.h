#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/core/base/events.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/public/api/protos/persqueue_error_codes_v1.pb.h>


namespace NKikimr::NPQ {

struct TEvPartitionChooser {
    enum EEv {
        EvInitResult = EventSpaceBegin(TKikimrEvents::ES_PQ_PARTITION_CHOOSER),

        EvChooseResult,
        EvChooseError,
        EvRefreshRequest,

        EvEnd,
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PQ_PARTITION_CHOOSER), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_PQ_PARTITION_CHOOSER)");

    struct TEvChooseResult: public TEventLocal<TEvChooseResult, EvChooseResult> {
        TEvChooseResult(ui32 partitionId, ui64 tabletId, std::optional<ui64> seqNo)
            : PartitionId(partitionId)
            , TabletId(tabletId)
            , SeqNo(seqNo) {
        }

        ui32 PartitionId;
        ui64 TabletId;
        std::optional<ui64> SeqNo;
    };

    struct TEvChooseError: public TEventLocal<TEvChooseError, EvChooseError>  {
        TEvChooseError(Ydb::PersQueue::ErrorCode::ErrorCode code, TString&& errorMessage)
            : Code(code)
            , ErrorMessage(std::move(errorMessage)) {
        }

        Ydb::PersQueue::ErrorCode::ErrorCode Code;
        TString ErrorMessage;
    };

    struct TEvRefreshRequest: public TEventLocal<TEvRefreshRequest, EvRefreshRequest> {
    };

};

class IPartitionChooser {
public:
    struct TPartitionInfo {
        TPartitionInfo(ui32 partitionId, ui64 tabletId)
            : PartitionId(partitionId)
            , TabletId(tabletId) {}

        ui32 PartitionId;
        ui64 TabletId;
    };

    virtual ~IPartitionChooser() = default;

    virtual const TPartitionInfo* GetPartition(const TString& sourceId) const = 0;
    virtual const TPartitionInfo* GetPartition(ui32 partitionId) const = 0;
    virtual const TPartitionInfo* GetRandomPartition() const = 0;
};

std::shared_ptr<IPartitionChooser> CreatePartitionChooser(const NKikimrSchemeOp::TPersQueueGroupDescription& config, bool withoutHash = false);

NActors::IActor* CreatePartitionChooserActor(TActorId parentId,
                                             const NKikimrSchemeOp::TPersQueueGroupDescription& config,
                                             NPersQueue::TTopicConverterPtr& fullConverter,
                                             const TString& sourceId,
                                             std::optional<ui32> preferedPartition,
                                             bool withoutHash = false);

} // namespace NKikimr::NPQ
