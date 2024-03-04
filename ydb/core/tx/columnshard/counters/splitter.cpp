#include "splitter.h"

namespace NKikimr::NColumnShard {

TSplitterCounters::TSplitterCounters(const TCommonCountersOwner& owner)
    : TBase(owner, "splitter")
    , SimpleSplitter(owner, "simple")
    , BySizeSplitter(owner, "by_size")
    , SplittedBlobs(owner, "splitted")
    , MonoBlobs(owner, "mono")
{
}

}
