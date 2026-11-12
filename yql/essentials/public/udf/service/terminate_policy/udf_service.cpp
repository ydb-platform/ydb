#include <yql/essentials/public/udf/udf_registrator.h>
#include <yql/essentials/public/udf/service/dispatch/udf_service_dispatch.h>

#include <util/system/compiler.h>

extern "C" [[noreturn]] void UdfTerminate(const char* message) {
    YqlUdfGetSymbols()->UdfTerminate(message);
    Y_UNREACHABLE();
}

extern "C" void* UdfAllocateWithSize(ui64 size) {
    return YqlUdfGetSymbols()->UdfAllocateWithSizeFunc(size);
}

extern "C" void UdfFreeWithSize(const void* memory, ui64 size) {
    return YqlUdfGetSymbols()->UdfFreeWithSizeFunc(memory, size);
}

extern "C" void* UdfArrowAllocate(ui64 size) {
    return YqlUdfGetSymbols()->UdfArrowAllocateFunc(size);
}

extern "C" void* UdfArrowReallocate(const void* memory, ui64 previousSize, ui64 size) {
    return YqlUdfGetSymbols()->UdfArrowReallocateFunc(memory, previousSize, size);
}

extern "C" void UdfArrowFree(const void* memory, ui64 size) {
    return YqlUdfGetSymbols()->UdfArrowFreeFunc(memory, size);
}

extern "C" void UdfRegisterObject(NYql::NUdf::TBoxedValue*) {
}

extern "C" void UdfUnregisterObject(NYql::NUdf::TBoxedValue*) {
}
