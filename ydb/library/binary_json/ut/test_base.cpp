#include "test_base.h"

#include <ydb/library/yql/minikql/dom/json.h>

using namespace NYql::NDom;

TBinaryJsonTestBase::TBinaryJsonTestBase()
    : FunctionRegistry(CreateFunctionRegistry(CreateBuiltinRegistry()))
    , Alloc(__LOCATION__)
    , Env(Alloc)
    , MemInfo("Memory")
    , HolderFactory(Alloc.Ref(), MemInfo, FunctionRegistry.Get())
    , ValueBuilder(HolderFactory)
{
}

TString TBinaryJsonTestBase::EntryToJsonText(const TEntryCursor& cursor) {
    if (cursor.GetType() == EEntryType::Container) {
        return ContainerToJsonText(cursor.GetContainer());
    }

    TUnboxedValue result = ReadElementToJsonDom(cursor, &ValueBuilder);
    return SerializeJsonDom(result);
}

TString TBinaryJsonTestBase::ContainerToJsonText(const TContainerCursor& cursor) {
    TUnboxedValue result = ReadContainerToJsonDom(cursor, &ValueBuilder);
    return SerializeJsonDom(result);
}
