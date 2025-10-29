#pragma once

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/public/udf/udf_types.h>
#include <yql/essentials/public/udf/udf_value.h>

#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/tuple.h>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/block_layout_converter.h>

namespace NKikimr::NMiniKQL {

class THolderFactory;

class IScalarLayoutConverter : private TNonCopyable {
public:
    using TPackResult = IBlockLayoutConverter::TPackResult;
    using TPackedTuple = std::vector<ui8, TMKQLAllocator<ui8>>;
    using TOverflow = std::vector<ui8, TMKQLAllocator<ui8>>;

public:
    using TPtr = std::unique_ptr<IScalarLayoutConverter>;

public:
    virtual ~IScalarLayoutConverter() = default;

    // Pack single tuple from scalar values
    virtual void Pack(const NYql::NUdf::TUnboxedValue* values, TPackResult& packed) = 0;
    
    // Unpack single tuple to scalar values
    virtual void Unpack(const TPackResult& packed, ui32 tupleIndex, NYql::NUdf::TUnboxedValue* values, const THolderFactory& holderFactory) = 0;
    
    virtual const NPackedTuple::TTupleLayout* GetTupleLayout() const = 0;
};

IScalarLayoutConverter::TPtr MakeScalarLayoutConverter(
    const NUdf::ITypeInfoHelper& typeInfoHelper, const TVector<TType*>& types,
    const TVector<NPackedTuple::EColumnRole>& roles);
}

