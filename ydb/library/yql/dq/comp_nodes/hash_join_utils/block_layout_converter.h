#pragma once

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/public/udf/udf_types.h>

#include <arrow/type.h>

#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/tuple.h>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/layout_converter_common.h>

namespace NKikimr::NMiniKQL {

class IBlockLayoutConverter : private TNonCopyable {
public:
    using TPackResult = ::NKikimr::NMiniKQL::TPackResult;
    using TPtr = std::unique_ptr<IBlockLayoutConverter>;

public:
    virtual ~IBlockLayoutConverter() = default;

    // Can be called multiple times to accumulate packed data in one storage
    virtual void Pack(const TVector<arrow::Datum>& columns, TPackResult& packed) = 0;
    virtual void BucketPack(const TVector<arrow::Datum>& columns, TPaddedPtr<TPackResult> packs, ui32 bucketsLogNum) = 0;
    // Can not be called multiple times due to immutability of arrow arrays
    virtual void Unpack(const TPackResult& packed, TVector<arrow::Datum>& columns) = 0;
    // virtual void UnpackApply(const TPackResult& packed, std::function<void(const char*)>);
    virtual const NPackedTuple::TTupleLayout* GetTupleLayout() const = 0;

    // Prints detailed debug information about the converter structure:
    // - Types of extractors (Fixed/Variable/Tuple/String/etc)
    // - Element sizes and nullable flags
    // - Inner extractors and their indices
    // - Mapping between outer and inner extractors
    // - TupleLayout details (columns, offsets, sizes, roles)
    //
    // Example usage:
    //   auto converter = MakeBlockLayoutConverter(...);
    //   converter->DebugPrint();
    virtual void DebugPrint() const = 0;

private:
};

IBlockLayoutConverter::TPtr MakeBlockLayoutConverter(
    const NUdf::ITypeInfoHelper& typeInfoHelper, const TVector<TType*>& types,
    const TVector<NPackedTuple::EColumnRole>& roles, arrow::MemoryPool* pool,
    bool rememberNullBitmaps = true);
}
