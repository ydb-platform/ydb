#include "column_engine.h"
#include "changes/abstract/abstract.h"
#include <util/stream/output.h>

namespace NKikimr::NOlap {

const std::shared_ptr<arrow::Schema>& IColumnEngine::GetReplaceKey() const {
    return GetVersionedIndex().GetLastSchema()->GetIndexInfo().GetReplaceKey();
}
}

template <>
void Out<NKikimr::NOlap::TColumnEngineChanges>(IOutputStream& out, TTypeTraits<NKikimr::NOlap::TColumnEngineChanges>::TFuncParam changes) {
    out << changes.DebugString();
}
