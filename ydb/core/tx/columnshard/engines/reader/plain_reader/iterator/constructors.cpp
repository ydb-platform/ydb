#include "constructors.h"
#include "source.h"

#include <ydb/core/tx/columnshard/engines/portions/written.h>

namespace NKikimr::NOlap::NReader::NPlain {

std::shared_ptr<NCommon::IDataSource> TPortionSources::DoTryExtractNext(
    const std::shared_ptr<NCommon::TSpecialReadContext>& context, const ui32 /*inFlightCurrentLimit*/) {
    auto result = std::make_shared<TPortionDataSource>(SourceIdx++, Sources.front(), std::static_pointer_cast<TSpecialReadContext>(context));
    Sources.pop_front();
    return result;
}

std::vector<TInsertWriteId> TPortionSources::GetUncommittedWriteIds() const {
    std::vector<TInsertWriteId> result;
    for (auto&& i : Sources) {
        if (!i->IsCommitted()) {
            AFL_VERIFY(i->GetPortionType() == EPortionType::Written);
            auto* written = static_cast<const TWrittenPortionInfo*>(i.get());
            result.emplace_back(written->GetInsertWriteId());
        }
    }
    return result;
}

}   // namespace NKikimr::NOlap::NReader::NPlain
