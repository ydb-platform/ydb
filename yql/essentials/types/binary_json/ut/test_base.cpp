#include "test_base.h"

#include <yql/essentials/minikql/dom/json.h>

using namespace NYql::NDom;

TBinaryJsonTestBase::TBinaryJsonTestBase()
    : FunctionRegistry_(CreateFunctionRegistry(CreateBuiltinRegistry()))
    , Alloc_(__LOCATION__)
    , Env_(Alloc_)
    , MemInfo_("Memory")
    , HolderFactory_(Alloc_.Ref(), MemInfo_, FunctionRegistry_.Get())
    , ValueBuilder_(HolderFactory_)
{
}

TString TBinaryJsonTestBase::EntryToJsonText(const TEntryCursor& cursor) {
    if (cursor.GetType() == EEntryType::Container) {
        return ContainerToJsonText(cursor.GetContainer());
    }

    TUnboxedValue result = ReadElementToJsonDom(cursor, &ValueBuilder_);
    return SerializeJsonDom(result);
}

TString TBinaryJsonTestBase::ContainerToJsonText(const TContainerCursor& cursor) {
    TUnboxedValue result = ReadContainerToJsonDom(cursor, &ValueBuilder_);
    return SerializeJsonDom(result);
}
