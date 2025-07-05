#include "constructors.h"
#include "source.h"

namespace NKikimr::NOlap::NReader::NPlain {

std::shared_ptr<NKikimr::NOlap::NReader::NCommon::IDataSource> NKikimr::NOlap::NReader::NPlain::TPortionSources::DoExtractNext(
    const std::shared_ptr<NCommon::TSpecialReadContext>& context) {
    auto result = std::make_shared<TPortionDataSource>(SourceIdx++, Sources.front(), std::static_pointer_cast<TSpecialReadContext>(context));
    Sources.pop_front();
    return result;
}

}   // namespace NKikimr::NOlap::NReader::NPlain
