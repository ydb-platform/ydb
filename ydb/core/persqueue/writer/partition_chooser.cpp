#include "partition_chooser.h"
#include "partition_chooser_impl.h"

#include <ydb/core/persqueue/public/partition_key_range/partition_key_range.h>
#include <ydb/services/lib/sharding/sharding.h>

namespace NKikimr::NPQ {
namespace NPartitionChooser {

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

} // namespace NKikimr::NPQ
