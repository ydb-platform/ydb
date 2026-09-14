#pragma once
#include "mkql_block_agg_factory.h"

namespace NKikimr::NMiniKQL {

std::unique_ptr<IBlockAggregatorFactory> MakeBlockSumFactory();
std::unique_ptr<IBlockAggregatorFactory> MakeBlockAvgFactory();

} // namespace NKikimr::NMiniKQL
