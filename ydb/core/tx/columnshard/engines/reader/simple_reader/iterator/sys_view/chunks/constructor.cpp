#include "constructor.h"
#include "source.h"

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NChunks {

std::shared_ptr<IDataSource> TPortionDataConstructor::Construct(const std::shared_ptr<NReader::NSimple::TSpecialReadContext>& context) {
    AFL_VERIFY(SourceId);
    return std::make_shared<TSourceData>(
        SourceId, SourceIdx, PathId, TabletId, Portion, std::move(Start), std::move(Finish), context, std::move(Schema));
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NChunks
