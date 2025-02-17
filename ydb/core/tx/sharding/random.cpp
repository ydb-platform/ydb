#include "random.h"

namespace NKikimr::NSharding {

THashMap<ui64, std::vector<ui32>> TRandomSharding::MakeSharding(const std::shared_ptr<arrow::RecordBatch>& batch) const {
    std::vector<ui64> activeShardIds;
    for (auto&& i : GetOrderedShardIds()) {
        if (IsActiveForWrite(i)) {
            activeShardIds.emplace_back(i);
        }
    }
    AFL_VERIFY(activeShardIds.size());
    THashMap<ui64, std::vector<ui32>> resultHash;
    std::vector<ui32>& sVector = resultHash.emplace(activeShardIds[RandomNumber<ui32>(activeShardIds.size())], std::vector<ui32>()).first->second;
    for (ui32 i = 0; i < batch->num_rows(); ++i) {
        sVector.emplace_back(i);
    }
    return resultHash;
}

}
