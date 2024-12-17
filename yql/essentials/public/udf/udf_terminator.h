#pragma once
#include <util/system/compiler.h>

namespace NYql {
namespace NUdf {
    class TBoxedValue;
}
}

extern "C" [[noreturn]] void UdfTerminate(const char* message);
extern "C" void UdfRegisterObject(::NYql::NUdf::TBoxedValue* object);
extern "C" void UdfUnregisterObject(::NYql::NUdf::TBoxedValue* object);
