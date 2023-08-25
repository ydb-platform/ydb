#include <library/cpp/testing/unittest/registar.h>

#include "mkql_functions.h"
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>

namespace NKikimr::NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLArrowFunctions) {
    Y_UNIT_TEST(Add) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto builtins = CreateBuiltinRegistry();

        auto uint64Type = TDataType::Create(NUdf::GetDataTypeInfo(NUdf::EDataSlot::Uint64).TypeId, env);
        auto uint64TypeOpt = TOptionalType::Create(uint64Type, env);

        auto scalarType = TBlockType::Create(uint64Type, TBlockType::EShape::Scalar, env);
        auto arrayType = TBlockType::Create(uint64Type, TBlockType::EShape::Many, env);

        auto scalarTypeOpt = TBlockType::Create(uint64TypeOpt, TBlockType::EShape::Scalar, env);
        auto arrayTypeOpt = TBlockType::Create(uint64TypeOpt, TBlockType::EShape::Many, env);

        UNIT_ASSERT(!FindArrowFunction("_Add_", {}, scalarType, *builtins));
        UNIT_ASSERT(!FindArrowFunction("Add", {}, scalarType, *builtins));
        UNIT_ASSERT(!FindArrowFunction("Add", TVector<TType*>{ scalarType }, scalarType, *builtins));
        UNIT_ASSERT(!FindArrowFunction("Add", TVector<TType*>{ arrayType }, arrayType, *builtins));
        UNIT_ASSERT(!FindArrowFunction("Add", TVector<TType*>{ scalarTypeOpt }, scalarType, *builtins));
        UNIT_ASSERT(!FindArrowFunction("Add", TVector<TType*>{ arrayTypeOpt }, arrayType, *builtins));

        UNIT_ASSERT(FindArrowFunction("Add", TVector<TType*>{ arrayType, arrayType }, arrayType, *builtins));
        UNIT_ASSERT(FindArrowFunction("Add", TVector<TType*>{ scalarType, arrayType }, arrayType, *builtins));
        UNIT_ASSERT(FindArrowFunction("Add", TVector<TType*>{ arrayType, scalarType }, arrayType, *builtins));
        UNIT_ASSERT(FindArrowFunction("Add", TVector<TType*>{ scalarType, scalarType }, scalarType, *builtins));

        UNIT_ASSERT(FindArrowFunction("Add", TVector<TType*>{ arrayType, arrayTypeOpt }, arrayTypeOpt, *builtins));
        UNIT_ASSERT(FindArrowFunction("Add", TVector<TType*>{ scalarType, arrayTypeOpt }, arrayTypeOpt, *builtins));
        UNIT_ASSERT(FindArrowFunction("Add", TVector<TType*>{ arrayType, scalarTypeOpt }, arrayTypeOpt, *builtins));
        UNIT_ASSERT(FindArrowFunction("Add", TVector<TType*>{ scalarType, scalarTypeOpt }, scalarTypeOpt, *builtins));

        UNIT_ASSERT(FindArrowFunction("Add", TVector<TType*>{ arrayTypeOpt, arrayType }, arrayTypeOpt, *builtins));
        UNIT_ASSERT(FindArrowFunction("Add", TVector<TType*>{ scalarTypeOpt, arrayType }, arrayTypeOpt, *builtins));
        UNIT_ASSERT(FindArrowFunction("Add", TVector<TType*>{ arrayTypeOpt, scalarType }, arrayTypeOpt, *builtins));
        UNIT_ASSERT(FindArrowFunction("Add", TVector<TType*>{ scalarTypeOpt, scalarType }, scalarTypeOpt, *builtins));

        UNIT_ASSERT(FindArrowFunction("Add", TVector<TType*>{ arrayTypeOpt, arrayTypeOpt }, arrayTypeOpt, *builtins));
        UNIT_ASSERT(FindArrowFunction("Add", TVector<TType*>{ scalarTypeOpt, arrayTypeOpt }, arrayTypeOpt, *builtins));
        UNIT_ASSERT(FindArrowFunction("Add", TVector<TType*>{ arrayTypeOpt, scalarTypeOpt }, arrayTypeOpt, *builtins));
        UNIT_ASSERT(FindArrowFunction("Add", TVector<TType*>{ scalarTypeOpt, scalarTypeOpt }, scalarTypeOpt, *builtins));
    }
}

}
