#pragma once

#include <util/generic/noncopyable.h>
#include <yql/essentials/public/udf/udf_types.h>

#include <arrow/type.h>

namespace NKikimr::NMiniKQL {

class IBlockLayoutConverter : private TNonCopyable {
public:
    struct PackResult {
        std::vector<ui8, TMKQLAllocator<ui8>> PackedTuples;
        std::vector<ui8, TMKQLAllocator<ui8>> Overflow;
        ui32 NTuples;
    };

public:
    using TPtr = std::unique_ptr<IBlockLayoutConverter>;

    virtual ~IBlockLayoutConverter() = default;

    virtual void Pack(const TVector<arrow::Datum>& columns, PackResult& packed) = 0;
    virtual void Unpack(const PackResult& packed, TVector<arrow::Datum>& columns) = 0;
    virtual const NPackedTuple::TTupleLayout* GetTupleLayout() const = 0;
};

IBlockLayoutConverter::TPtr MakeBlockLayoutConverter(
    const NUdf::ITypeInfoHelper& typeInfoHelper, const Tvector<NUdf::TType*>& types,
    const TVector<NPackedTuple::EColumnRole>& roles, arrow::MemoryPool* pool);

}
