#include "partition_chooser_impl.h"

#include <library/cpp/digest/md5/md5.h>
#include <ydb/core/persqueue/partition_key_range/partition_key_range.h>
#include <ydb/core/persqueue/utils.h>
#include <ydb/services/lib/sharding/sharding.h>

namespace NKikimr::NPQ {
namespace NPartitionChooser {

NYql::NDecimal::TUint128 Hash(const TString& sourceId) {
    return NKikimr::NDataStreams::V1::HexBytesToDecimal(MD5::Calc(sourceId));
}

ui32 TAsIsSharder::operator()(const TString& sourceId, ui32 totalShards) const {
    return NKikimr::NDataStreams::V1::ShardFromDecimal(AsInt<NYql::NDecimal::TUint128>(sourceId), totalShards);
}

ui32 TMd5Sharder::operator()(const TString& sourceId, ui32 totalShards) const {
    return NKikimr::NDataStreams::V1::ShardFromDecimal(Hash(sourceId), totalShards);
}

TString TAsIsConverter::operator()(const TString& sourceId) const {
    return sourceId;
}

TString TMd5Converter::operator()(const TString& sourceId) const {
    return AsKeyBound(Hash(sourceId));
}

} // namespace NPartitionChooser


std::shared_ptr<IPartitionChooser> CreatePartitionChooser(const NKikimrSchemeOp::TPersQueueGroupDescription& config, bool withoutHash) {
    if (SplitMergeEnabled(config.GetPQTabletConfig())) {
        if (withoutHash) {
            return std::make_shared<NPartitionChooser::TBoundaryChooser<NPartitionChooser::TAsIsConverter>>(config);
        } else {
            return std::make_shared<NPartitionChooser::TBoundaryChooser<NPartitionChooser::TMd5Converter>>(config);
        }
    } else {
        if (withoutHash) {
            return std::make_shared<NPartitionChooser::THashChooser<NPartitionChooser::TAsIsSharder>>(config);
        } else {
            return std::make_shared<NPartitionChooser::THashChooser<NPartitionChooser::TMd5Sharder>>(config);
        }
    }
}

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
        return new NPartitionChooser::TSMPartitionChooserActor<TPipeHelper>(parentId, chooser, graph, fullConverter, sourceId, preferedPartition, std::move(traceId));
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
