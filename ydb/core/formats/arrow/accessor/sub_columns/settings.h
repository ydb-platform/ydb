#pragma once
#include <ydb/core/formats/arrow/arrow_helpers.h>

#include <ydb/library/formats/arrow/accessor/abstract/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_base.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

class TSettings {
public:
    static const ui32 ColumnAccessorsCountLimit = 1024;
};

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
