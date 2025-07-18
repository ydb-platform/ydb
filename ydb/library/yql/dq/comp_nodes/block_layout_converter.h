#pragma once

#include <util/generic/noncopyable.h>
#include <yql/essentials/public/udf/udf_types.h>

#include <arrow/type.h>

#include <ydb/library/yql/minikql/comp_nodes/packed_tuple/tuple.h>

namespace NKikimr::NMiniKQL {

class IBlockLayoutConverter : private TNonCopyable {
public:
    struct TPackResult {
        std::vector<ui8, TMKQLAllocator<ui8>> PackedTuples;
        std::vector<ui8, TMKQLAllocator<ui8>> Overflow;
        ui32 NTuples{0};
    };

    using TPackedTuple = std::vector<ui8, TMKQLAllocator<ui8>>;
    using TOverflow = std::vector<ui8, TMKQLAllocator<ui8>>;

public:
    using TPtr = std::unique_ptr<IBlockLayoutConverter>;

public:
    virtual ~IBlockLayoutConverter() = default;

    // Can be called multiple times to accumulate packed data in one storage
    virtual void Pack(const TVector<arrow::Datum>& columns, TPackResult& packed) = 0;
    virtual void BucketPack(const TVector<arrow::Datum>& columns, TPaddedPtr<TPackResult> packs, ui32 bucketsLogNum) = 0;
    // Can not be called multiple times due to immutability of arrow arrays
    virtual void Unpack(const TPackResult& packed, TVector<arrow::Datum>& columns) = 0;
    virtual const NPackedTuple::TTupleLayout* GetTupleLayout() const = 0;
};

IBlockLayoutConverter::TPtr MakeBlockLayoutConverter(
    const NUdf::ITypeInfoHelper& typeInfoHelper, const TVector<TType*>& types,
    const TVector<NPackedTuple::EColumnRole>& roles, arrow::MemoryPool* pool);

}
