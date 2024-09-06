#pragma once

#include <util/random/random.h>
#include <ydb/core/persqueue/utils.h>

#include "partition_chooser_impl__old_chooser_actor.h"
#include "partition_chooser_impl__sm_chooser_actor.h"


namespace NKikimr::NPQ {
namespace NPartitionChooser {

struct TAsIsSharder {
    ui32 operator()(const TString& sourceId, ui32 totalShards) const;
};

struct TMd5Sharder {
    ui32 operator()(const TString& sourceId, ui32 totalShards) const;
};

struct TAsIsConverter {
    TString operator()(const TString& sourceId) const;
};

struct TMd5Converter {
    TString operator()(const TString& sourceId) const;
};

// Chooses the partition to produce messages using the boundaries of the partition for the SourceID distribution.
// It used for split/merge distribution and guarantee stable distribution for changing partition set.
template<class THasher = TMd5Converter>
class TBoundaryChooser: public IPartitionChooser {
public:
    struct TPartitionInfo: public IPartitionChooser::TPartitionInfo {
        TPartitionInfo(ui32 partitionId, ui64 tabletId, std::optional<TString> toBound)
            : IPartitionChooser::TPartitionInfo(partitionId, tabletId)
            , ToBound(toBound) {}

        std::optional<TString> ToBound;
    };

    TBoundaryChooser(const NKikimrSchemeOp::TPersQueueGroupDescription& config);
    TBoundaryChooser(TBoundaryChooser&&) = default;

    const TPartitionInfo* GetPartition(const TString& sourceId) const override;
    const TPartitionInfo* GetPartition(ui32 partitionId) const override;
    const TPartitionInfo* GetRandomPartition() const override;

private:
    const TString TopicName;
    std::vector<TPartitionInfo> Partitions;
    THasher Hasher;
};

// It is old alghoritm of choosing partition by SourceId
template<typename THasher = TMd5Sharder>
class THashChooser: public IPartitionChooser {
public:
    THashChooser(const NKikimrSchemeOp::TPersQueueGroupDescription& config);

    const TPartitionInfo* GetPartition(const TString& sourceId) const override;
    const TPartitionInfo* GetPartition(ui32 partitionId) const override;
    const TPartitionInfo* GetRandomPartition() const override;

private:
    std::vector<TPartitionInfo> Partitions;
    THasher Hasher;
};


//
// TBoundaryChooser
//

template<class THasher>
TBoundaryChooser<THasher>::TBoundaryChooser(const NKikimrSchemeOp::TPersQueueGroupDescription& config)
    : TopicName(config.GetPQTabletConfig().GetTopicName()) {
    for(const auto& p : config.GetPartitions()) {
        if (NKikimrPQ::ETopicPartitionStatus::Active == p.GetStatus()) {
            auto toBound = p.HasKeyRange() && p.GetKeyRange().HasToBound() ?
                            std::optional<TString>(p.GetKeyRange().GetToBound()) : std::nullopt;
            Partitions.emplace_back(TPartitionInfo{p.GetPartitionId(),
                                    p.GetTabletId(),
                                    toBound});
        }
    }

    std::sort(Partitions.begin(), Partitions.end(),
        [](const TPartitionInfo& a, const TPartitionInfo& b) { return !b.ToBound || (a.ToBound && a.ToBound < b.ToBound); });
}

template<class THasher>
const typename TBoundaryChooser<THasher>::TPartitionInfo* TBoundaryChooser<THasher>::GetPartition(const TString& sourceId) const {
    const auto keyHash = Hasher(sourceId);
    auto result = std::upper_bound(Partitions.begin(), Partitions.end(), keyHash,
                    [](const TString& value, const TPartitionInfo& partition) { return !partition.ToBound || value < partition.ToBound; });
    Y_ABORT_UNLESS(result != Partitions.end(), "Partition not found. Maybe wrong partitions bounds. Topic '%s'", TopicName.c_str());
    return result;
}

template<class THasher>
const typename TBoundaryChooser<THasher>::TPartitionInfo* TBoundaryChooser<THasher>::GetPartition(ui32 partitionId) const {
    auto it = std::find_if(Partitions.begin(), Partitions.end(),
        [=](const TPartitionInfo& v) { return v.PartitionId == partitionId; });
    return it == Partitions.end() ? nullptr : it;
}

template<class THasher>
const typename TBoundaryChooser<THasher>::TPartitionInfo* TBoundaryChooser<THasher>::GetRandomPartition() const {
    if (Partitions.empty()) {
        return nullptr;
    }
    size_t p = RandomNumber<size_t>(Partitions.size());
    return &Partitions[p];
}



//
// THashChooser
//
template<class THasher>
THashChooser<THasher>::THashChooser(const NKikimrSchemeOp::TPersQueueGroupDescription& config) {
    for(const auto& p : config.GetPartitions()) {
        if (NKikimrPQ::ETopicPartitionStatus::Active == p.GetStatus()) {
            Partitions.emplace_back(TPartitionInfo{p.GetPartitionId(),
                                    p.GetTabletId()});
        }
    }

    std::sort(Partitions.begin(), Partitions.end(),
        [](const TPartitionInfo& a, const TPartitionInfo& b) { return a.PartitionId < b.PartitionId; });
}

template<class THasher>
const typename THashChooser<THasher>::TPartitionInfo* THashChooser<THasher>::GetPartition(const TString& sourceId) const {
    if (Partitions.empty()) {
        return nullptr;
    }
    return &Partitions[Hasher(sourceId, Partitions.size())];
}

template<class THasher>
const typename THashChooser<THasher>::TPartitionInfo* THashChooser<THasher>::GetPartition(ui32 partitionId) const {
    auto it = std::lower_bound(Partitions.begin(), Partitions.end(), partitionId,
                    [](const TPartitionInfo& partition, const ui32 value) { return value > partition.PartitionId; });
    if (it == Partitions.end()) {
        return nullptr;
    }
    return it->PartitionId == partitionId ? it : nullptr;
}

template<class THasher>
const typename THashChooser<THasher>::TPartitionInfo* THashChooser<THasher>::GetRandomPartition() const {
    if (Partitions.empty()) {
        return nullptr;
    }
    size_t p = RandomNumber<size_t>(Partitions.size());
    return &Partitions[p];
}


} // namespace NPartitionChooser


inline IActor* CreatePartitionChooserActorM(TActorId parentId,
                                    const NKikimrSchemeOp::TPersQueueGroupDescription& config,
                                    NPersQueue::TTopicConverterPtr& fullConverter,
                                    const TString& sourceId,
                                    std::optional<ui32> preferedPartition,
                                    bool withoutHash) {
    auto chooser = CreatePartitionChooser(config, withoutHash);
    if (SplitMergeEnabled(config.GetPQTabletConfig())) {
        return new NPartitionChooser::TSMPartitionChooserActor<NTabletPipe::NTest::TPipeMock>(parentId, config, chooser, fullConverter, sourceId, preferedPartition);
    } else {
        return new NPartitionChooser::TPartitionChooserActor<NTabletPipe::NTest::TPipeMock>(parentId, config, chooser, fullConverter, sourceId, preferedPartition);
    }
}


} // namespace NKikimr::NPQ
