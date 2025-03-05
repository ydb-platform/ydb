#pragma once

#include <ydb/core/formats/arrow/arrow_filter.h>

namespace NKikimr::NOlap::NReader {

class IFilterSubscriber {
public:
    virtual void OnFilterReady(const NArrow::TColumnFilter&) = 0;
    virtual ~IFilterSubscriber() = default;
};

}   // namespace NKikimr::NOlap::NReader
