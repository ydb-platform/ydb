#include "sglist_test.h"

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

TSgList ResizeBlocks(TVector<TString>& blocks, ui64 blocksCount,
                     const TString& blockContent)
{
    blocks.clear();
    blocks.reserve(blocksCount);

    TSgList sglist;
    sglist.reserve(blocksCount);

    for (ui64 i = 0; i < blocksCount; ++i) {
        blocks.emplace_back(blockContent.data(), blockContent.size());
        sglist.emplace_back(blocks.back().data(), blocks.back().size());
    }

    return sglist;
}

}   // namespace NYdb::NBS
