#pragma once

#include <util/generic/noncopyable.h>
#include <yql/essentials/public/udf/udf_types.h>

#include <arrow/type.h>

namespace NKikimr::NMiniKQL {

class IBlockTrimmer : private TNonCopyable {
public:
    using TPtr = std::unique_ptr<IBlockTrimmer>;

    virtual ~IBlockTrimmer() = default;

    virtual std::shared_ptr<arrow::ArrayData> Trim(const std::shared_ptr<arrow::ArrayData>& array) = 0;
};

IBlockTrimmer::TPtr MakeBlockTrimmer(const NYql::NUdf::ITypeInfoHelper& typeInfoHelper, const NYql::NUdf::TType* type, arrow::MemoryPool* pool);

}
