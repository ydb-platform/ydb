#include "constructor.h"

#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NSchemas {

TConstructor::TConstructor(
    const IColumnEngine& engine, const ui64 tabletId, const std::shared_ptr<NOlap::TPKRangesFilter>& pkFilter, const ERequestSorting sorting)
    : TBase(sorting, tabletId) {
    const TColumnEngineForLogs* engineImpl = dynamic_cast<const TColumnEngineForLogs*>(&engine);
    std::vector<ISnapshotSchema::TPtr> schemasAll;
    for (auto&& i : engineImpl->GetVersionedSchemas().GetPresetVersionedIndex()) {
        for (auto&& [_, s] : i.second->GetSchemaByVersion()) {
            schemasAll.emplace_back(s.GetSchema());
        }
    }
    const auto pred = [](const ISnapshotSchema::TPtr& l, const ISnapshotSchema::TPtr& r) {
        return std::tuple(l->GetIndexInfo().GetPresetId(), l->GetVersion()) < std::tuple(r->GetIndexInfo().GetPresetId(), r->GetVersion());
    };
    std::sort(schemasAll.begin(), schemasAll.end(), pred);
    std::vector<ISnapshotSchema::TPtr> current;
    std::deque<TDataSourceConstructor> constructors;
    for (auto&& i : schemasAll) {
        if (current.size() && current.back()->GetPresetId() != i->GetPresetId()) {
            constructors.emplace_back(TabletId, std::move(current));
            if (!pkFilter->IsUsed(constructors.back().GetStart(), constructors.back().GetFinish())) {
                constructors.pop_back();
            }
            current.clear();
        }
    }
    if (current.size()) {
        constructors.emplace_back(TabletId, std::move(current));
        if (!pkFilter->IsUsed(constructors.back().GetStart(), constructors.back().GetFinish())) {
            constructors.pop_back();
        }
        current.clear();
    }
    Constructors.Initialize(std::move(constructors));
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NSchemas
