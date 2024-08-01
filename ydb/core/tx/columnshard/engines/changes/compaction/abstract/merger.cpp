#include "merger.h"

namespace NKikimr::NOlap::NCompaction {

void IColumnMerger::Start(const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& input) {
    AFL_VERIFY(!Started);
    Started = true;
    // for (auto&& i : input) {
    //     AFL_VERIFY(i->GetDataType()->id() == Context.GetResultField()->type()->id())("input", i->GetDataType()->ToString())(
    //                                              "result", Context.GetResultField()->ToString());
    // }
    return DoStart(input);
}

}
