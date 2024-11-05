#define BUILD_UDF
#include <util/system/backtrace.h>

#if defined(_win_) || defined(_darwin_)
#include <ydb/library/yql/public/udf/udf_registrator.h>

#include <exception>

static NYql::NUdf::TStaticSymbols Symbols;

extern "C" void* UdfAllocate(ui64 size) {
    return Symbols.UdfAllocateFunc(size);
}

extern "C" void UdfFree(const void* mem) {
    return Symbols.UdfFreeFunc(mem);
}

extern "C" [[noreturn]] void UdfTerminate(const char* message) {
    Symbols.UdfTerminate(message);
    std::terminate();
}

extern "C" void UdfRegisterObject(::NYql::NUdf::TBoxedValue* object) {
    return Symbols.UdfRegisterObject(object);
}

extern "C" void UdfUnregisterObject(::NYql::NUdf::TBoxedValue* object) {
    return Symbols.UdfUnregisterObject(object);
}

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 8)
extern "C" void* UdfAllocateWithSize(ui64 size) {
    return Symbols.UdfAllocateWithSizeFunc(size);
}

extern "C" void UdfFreeWithSize(const void* mem, ui64 size) {
    return Symbols.UdfFreeWithSizeFunc(mem, size);
}
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 37)
extern "C" void* UdfArrowAllocate(ui64 size) { 
    return Symbols.UdfArrowAllocateFunc(size);
}

extern "C" void* UdfArrowReallocate(const void* mem, ui64 prevSize, ui64 size) { 
    return Symbols.UdfArrowReallocateFunc(mem, prevSize, size);
}

extern "C" void UdfArrowFree(const void* mem, ui64 size) {
    return Symbols.UdfArrowFreeFunc(mem, size);
}
#endif

extern "C" void BindSymbols(const NYql::NUdf::TStaticSymbols& symbols) {
    Symbols = symbols;
}
#endif

namespace NYql {
namespace NUdf {

typedef void(*TBackTraceCallback)();
static TBackTraceCallback BackTraceCallback;

static void UdfBackTraceFn(IOutputStream*, void* const*, size_t) {
    BackTraceCallback();
}

void SetBackTraceCallbackImpl(TBackTraceCallback callback) {
    BackTraceCallback = callback;
    SetFormatBackTraceFn(UdfBackTraceFn);
}

}
}
