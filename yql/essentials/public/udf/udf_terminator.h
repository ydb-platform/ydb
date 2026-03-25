#pragma once
#include <util/system/compiler.h>

namespace NYql::NUdf {
class TBoxedValue;
} // namespace NYql::NUdf

extern "C" [[noreturn]] void UdfTerminate(const char* message);
extern "C" void UdfRegisterObject(::NYql::NUdf::TBoxedValue* object);
extern "C" void UdfUnregisterObject(::NYql::NUdf::TBoxedValue* object);
