#include "partition_chooser_impl.h"

#include "partition_chooser_impl__old_chooser_actor.h"
#include "partition_chooser_impl__sm_chooser_actor.h"

#include <ydb/core/persqueue/public/partition_key_range/partition_key_range.h>

namespace NKikimr::NPQ {

template<typename TPipeHelper>
IActor* CreatePartitionChooserActor(TActorId parentId,
                                    const NKikimrSchemeOp::TPersQueueGroupDescription& config,
                                    const std::shared_ptr<NPQ::IPartitionChooser>& chooser,
                                    const std::shared_ptr<NPQ::TPartitionGraph>& graph,
                                    NPersQueue::TTopicConverterPtr& fullConverter,
                                    const TString& sourceId,
                                    std::optional<ui32> preferedPartition,
                                    NWilson::TTraceId traceId) {
    if (SplitMergeEnabled(config.GetPQTabletConfig())) {
        return new NPartitionChooser::TSMPartitionChooserActor<TPipeHelper>(parentId, config, chooser, graph, fullConverter, sourceId, preferedPartition, std::move(traceId));
    } else {
        return new NPartitionChooser::TPartitionChooserActor<TPipeHelper>(parentId, config, chooser, fullConverter, sourceId, preferedPartition, std::move(traceId));
    }
}

template
IActor* CreatePartitionChooserActor<NTabletPipe::TPipeHelper>(TActorId parentId,
                                    const NKikimrSchemeOp::TPersQueueGroupDescription& config,
                                    const std::shared_ptr<NPQ::IPartitionChooser>& chooser,
                                    const std::shared_ptr<NPQ::TPartitionGraph>& graph,
                                    NPersQueue::TTopicConverterPtr& fullConverter,
                                    const TString& sourceId,
                                    std::optional<ui32> preferedPartition,
                                    NWilson::TTraceId traceId);

template
IActor* CreatePartitionChooserActor<NTabletPipe::NTest::TPipeMock>(TActorId parentId,
                                    const NKikimrSchemeOp::TPersQueueGroupDescription& config,
                                    const std::shared_ptr<NPQ::IPartitionChooser>& chooser,
                                    const std::shared_ptr<NPQ::TPartitionGraph>& graph,
                                    NPersQueue::TTopicConverterPtr& fullConverter,
                                    const TString& sourceId,
                                    std::optional<ui32> preferedPartition,
                                    NWilson::TTraceId traceId);

} // namespace NKikimr::NPQ

std::unordered_map<ui64, NActors::TActorId> NKikimr::NTabletPipe::NTest::TPipeMock::Tablets;
