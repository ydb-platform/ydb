#include "constructors.h"

#include <ydb/core/tx/columnshard/engines/portions/written.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/source.h>

namespace NKikimr::NOlap::NReader::NSimple {

void TPortionsSources::DoInitCursor(const std::shared_ptr<IScanCursor>& cursor) {
    while (TBase::GetConstructorsCount()) {
        bool usage = false;
        if (!cursor->CheckEntityIsBorder(TBase::MutableNextConstructor(), usage)) {
            TBase::DropNextConstructor();
            continue;
        }
        if (usage) {
            TBase::MutableNextConstructor().SetIsStartedByCursor();
        } else {
            TBase::DropNextConstructor();
        }
        break;
    }
}

std::vector<TInsertWriteId> TPortionsSources::GetUncommittedWriteIds() const {
    std::vector<TInsertWriteId> result;
    for (auto&& i : TBase::GetConstructors()) {
        if (!i.GetPortion()->IsCommitted()) {
            AFL_VERIFY(i.GetPortion()->GetPortionType() == EPortionType::Written);
            auto* written = static_cast<const TWrittenPortionInfo*>(i.GetPortion().get());
            result.emplace_back(written->GetInsertWriteId());
        }
    }
    return result;
}

std::shared_ptr<TPortionDataSource> TSourceConstructor::Construct(
    const std::shared_ptr<NCommon::TSpecialReadContext>& context, std::shared_ptr<TPortionDataAccessor>&& accessor) const {
    AFL_VERIFY(SourceIdx);
    auto result = std::make_shared<TPortionDataSource>(*SourceIdx, Portion, context);
    result->SetPortionAccessor(std::move(accessor));
    if (IsStartedByCursorFlag) {
        result->SetIsStartedByCursor();
    }
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, result->AddEvent("s"));
    return result;
}

}   // namespace NKikimr::NOlap::NReader::NSimple
