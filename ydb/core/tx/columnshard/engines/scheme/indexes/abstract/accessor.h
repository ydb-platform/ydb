#pragma once

#include <util/system/types.h>

namespace NKikimr::NOlap {

class IColumnIndexAccessor {
public:
    virtual bool IsIndexInheritPortionStorage(const ui64 indexId) const = 0;
    virtual ~IColumnIndexAccessor() = default;
};

}   // namespace NKikimr::NOlap
