#pragma once
#include <ydb/core/tx/columnshard/common/path_id.h>

namespace NKikimr::NColumnShard {

struct IInternalToSsLocalPathId {
    virtual NColumnShard::TSsLo ResolveSsLocalPathId(NColumnShard::TInternalPathId) const = 0;
};

struct ISsLocalToInternalPathId {
    virtual NColumnShard::TInternalPathId ResolveInternalPathId() const = 0;
};

class TInternalPathIdMapper:
    public  IInternalToSsLocalPathId,
    public ISsLocalToInternalPathId,
public:
    virtual NColumnShard::TInternalPathId ResolveSsLocalPathId() const override {

    }

};

} //namespace NKikimr::NColumnShard