#include "constructor.h"

#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NSchemas {

TConstructor::TConstructor(
    const IColumnEngine& engine, const ui64 tabletId, const std::shared_ptr<NOlap::TPKRangesFilter>& pkFilter, const bool isReverseSort)
    : TabletId(tabletId) {
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
    for (auto&& i : schemasAll) {
        current.emplace_back(i);
        if (current.size() == 10) {
            AddConstructors(std::move(current), pkFilter);
            current.clear();
        }
    }
    if (current.size()) {
        AddConstructors(std::move(current), pkFilter);
        current.clear();
    }

    std::sort(Constructors.begin(), Constructors.end(), TDataConstructor::TComparator(isReverseSort));
    for (ui32 idx = 0; idx < Constructors.size(); ++idx) {
        Constructors[idx].SetIndex(idx);
    }
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NSchemas
