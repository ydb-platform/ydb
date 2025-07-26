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
    return Uncommitted;
}

}   // namespace NKikimr::NOlap::NReader::NPlain
