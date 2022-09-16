#include <library/cpp/testing/unittest/registar.h>

#include "mkql_functions.h"

namespace NKikimr::NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLArrowFunctions) {
    Y_UNIT_TEST(Add) {
        TScopedAlloc alloc;
        TTypeEnvironment env(alloc);

        auto uint64Type = TDataType::Create(NUdf::GetDataTypeInfo(NUdf::EDataSlot::Uint64).TypeId, env);
        auto uint64TypeOpt = TOptionalType::Create(uint64Type, env);

        auto scalarType = TBlockType::Create(uint64Type, TBlockType::EShape::Scalar, env);
        auto arrayType = TBlockType::Create(uint64Type, TBlockType::EShape::Many, env);

        auto scalarTypeOpt = TBlockType::Create(uint64TypeOpt, TBlockType::EShape::Scalar, env);
        auto arrayTypeOpt = TBlockType::Create(uint64TypeOpt, TBlockType::EShape::Many, env);

        TType* outputType;
        UNIT_ASSERT(!FindArrowFunction("_add_", {}, outputType, env));
        UNIT_ASSERT(!FindArrowFunction("add", {}, outputType, env));
        UNIT_ASSERT(!FindArrowFunction("add", TVector<TType*>{ scalarType }, outputType, env));
        UNIT_ASSERT(!FindArrowFunction("add", TVector<TType*>{ arrayType }, outputType, env));
        UNIT_ASSERT(!FindArrowFunction("add", TVector<TType*>{ scalarTypeOpt }, outputType, env));
        UNIT_ASSERT(!FindArrowFunction("add", TVector<TType*>{ arrayTypeOpt }, outputType, env));

        UNIT_ASSERT(FindArrowFunction("add", TVector<TType*>{ arrayType, arrayType }, outputType, env));
        UNIT_ASSERT(outputType->Equals(*arrayType));
        UNIT_ASSERT(FindArrowFunction("add", TVector<TType*>{ scalarType, arrayType }, outputType, env));
        UNIT_ASSERT(outputType->Equals(*arrayType));
        UNIT_ASSERT(FindArrowFunction("add", TVector<TType*>{ arrayType, scalarType }, outputType, env));
        UNIT_ASSERT(outputType->Equals(*arrayType));
        UNIT_ASSERT(FindArrowFunction("add", TVector<TType*>{ scalarType, scalarType }, outputType, env));
        UNIT_ASSERT(outputType->Equals(*scalarType));

        UNIT_ASSERT(FindArrowFunction("add", TVector<TType*>{ arrayType, arrayTypeOpt }, outputType, env));
        UNIT_ASSERT(outputType->Equals(*arrayTypeOpt));
        UNIT_ASSERT(FindArrowFunction("add", TVector<TType*>{ scalarType, arrayTypeOpt }, outputType, env));
        UNIT_ASSERT(outputType->Equals(*arrayTypeOpt));
        UNIT_ASSERT(FindArrowFunction("add", TVector<TType*>{ arrayType, scalarTypeOpt }, outputType, env));
        UNIT_ASSERT(outputType->Equals(*arrayTypeOpt));
        UNIT_ASSERT(FindArrowFunction("add", TVector<TType*>{ scalarType, scalarTypeOpt }, outputType, env));
        UNIT_ASSERT(outputType->Equals(*scalarTypeOpt));

        UNIT_ASSERT(FindArrowFunction("add", TVector<TType*>{ arrayTypeOpt, arrayType }, outputType, env));
        UNIT_ASSERT(outputType->Equals(*arrayTypeOpt));
        UNIT_ASSERT(FindArrowFunction("add", TVector<TType*>{ scalarTypeOpt, arrayType }, outputType, env));
        UNIT_ASSERT(outputType->Equals(*arrayTypeOpt));
        UNIT_ASSERT(FindArrowFunction("add", TVector<TType*>{ arrayTypeOpt, scalarType }, outputType, env));
        UNIT_ASSERT(outputType->Equals(*arrayTypeOpt));
        UNIT_ASSERT(FindArrowFunction("add", TVector<TType*>{ scalarTypeOpt, scalarType }, outputType, env));
        UNIT_ASSERT(outputType->Equals(*scalarTypeOpt));

        UNIT_ASSERT(FindArrowFunction("add", TVector<TType*>{ arrayTypeOpt, arrayTypeOpt }, outputType, env));
        UNIT_ASSERT(outputType->Equals(*arrayTypeOpt));
        UNIT_ASSERT(FindArrowFunction("add", TVector<TType*>{ scalarTypeOpt, arrayTypeOpt }, outputType, env));
        UNIT_ASSERT(outputType->Equals(*arrayTypeOpt));
        UNIT_ASSERT(FindArrowFunction("add", TVector<TType*>{ arrayTypeOpt, scalarTypeOpt }, outputType, env));
        UNIT_ASSERT(outputType->Equals(*arrayTypeOpt));
        UNIT_ASSERT(FindArrowFunction("add", TVector<TType*>{ scalarTypeOpt, scalarTypeOpt }, outputType, env));
        UNIT_ASSERT(outputType->Equals(*scalarTypeOpt));
    }

    Y_UNIT_TEST(IsNull) {
        TScopedAlloc alloc;
        TTypeEnvironment env(alloc);

        auto bool64Type = TDataType::Create(NUdf::GetDataTypeInfo(NUdf::EDataSlot::Bool).TypeId, env);
        auto bool64TypeOpt = TOptionalType::Create(bool64Type, env);

        auto scalarType = TBlockType::Create(bool64Type, TBlockType::EShape::Scalar, env);
        auto arrayType = TBlockType::Create(bool64Type, TBlockType::EShape::Many, env);

        auto scalarTypeOpt = TBlockType::Create(bool64TypeOpt, TBlockType::EShape::Scalar, env);
        auto arrayTypeOpt = TBlockType::Create(bool64TypeOpt, TBlockType::EShape::Many, env);

        TType* outputType;
        UNIT_ASSERT(!FindArrowFunction("is_null", {}, outputType, env));
        UNIT_ASSERT(!FindArrowFunction("is_null", TVector<TType*>{ scalarType, scalarType }, outputType, env));

        UNIT_ASSERT(FindArrowFunction("is_null", TVector<TType*>{ scalarType }, outputType, env));
        UNIT_ASSERT(outputType->Equals(*scalarType));
        UNIT_ASSERT(FindArrowFunction("is_null", TVector<TType*>{ arrayType }, outputType, env));
        UNIT_ASSERT(outputType->Equals(*arrayType));

        UNIT_ASSERT(FindArrowFunction("is_null", TVector<TType*>{ scalarTypeOpt }, outputType, env));
        UNIT_ASSERT(outputType->Equals(*scalarType));
        UNIT_ASSERT(FindArrowFunction("is_null", TVector<TType*>{ arrayTypeOpt }, outputType, env));
        UNIT_ASSERT(outputType->Equals(*arrayType));
    }
}

}
