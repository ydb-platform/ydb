#pragma once

#include <ydb/core/formats/arrow/arrow_filter.h>

namespace NKikimr::NOlap::NReader::NSimple {

class IFilterSubscriber {
public:
    virtual void OnFilterReady(const NArrow::TColumnFilter&) = 0;
    virtual void OnFailure(const TString& reason) = 0;
    virtual ~IFilterSubscriber() = default;
};

}   // namespace NKikimr::NOlap::NReader::NSimple
