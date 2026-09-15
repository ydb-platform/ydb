#pragma once

#include "partition.h"

namespace NKikimr::NPQ {

template<typename T>
std::function<void(bool, T& r)> TPartition::GetResultPostProcessor(const TString& consumer) {
    return [&, this](bool readingFinished, T& r) {
        r.SetReadingFinished(readingFinished);
        if (readingFinished) {
            ui32 partitionId = Partition.OriginalPartitionId;

            auto* node = PartitionGraph.GetPartition(partitionId);
            for (auto* child : node->DirectChildren) {
                r.AddChildPartitionIds(child->Id);

                for (auto* p : child->DirectParents) {
                    if (p->Id != partitionId) {
                        r.AddAdjacentPartitionIds(p->Id);
                    }
                }
            }

            if constexpr (std::is_same<T, NKikimrClient::TCmdReadResult>::value) {
                if (consumer) {
                    auto& userInfo = UsersInfoStorage->GetOrCreate(consumer, ActorContext());
                    r.SetCommittedToEnd(LastOffsetHasBeenCommited(userInfo));
                }
            }
        }
    };
}

}
