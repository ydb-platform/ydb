#pragma once
#include <ydb/core/tx/columnshard/common/path_id.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/accessor/positive_integer.h>
#include <ydb/library/actors/core/monotonic.h>

#include <util/generic/string.h>
#include <util/stream/output.h>
#include <util/system/yassert.h>

#include <memory>

namespace NKikimr::NOlap {
class TGranuleMeta;

class TPlanCompactionInfo {
private:
    TInternalPathId PathId;
    TMonotonic StartTime = TMonotonic::Now();
    TPositiveControlInteger Count;
    YDB_READONLY_DEF(TString, TaskId);

public:
    void Start() {
        StartTime = TMonotonic::Now();
        ++Count;
    }

    bool Finish();

    TMonotonic GetStartTime() const {
        return StartTime;
    }

    explicit TPlanCompactionInfo(const TInternalPathId pathId, const TString& taskId)
        : PathId(pathId)
        , TaskId(taskId) {
    }

    TInternalPathId GetPathId() const {
        return PathId;
    }
};

}   // namespace NKikimr::NOlap
