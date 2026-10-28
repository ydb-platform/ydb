#define BUILD_UDF
#include <util/system/backtrace.h>

#if defined(_win_) || defined(_darwin_)
    #include <yql/essentials/public/udf/udf_registrator.h>

    #include <exception>

static NYql::NUdf::TStaticSymbols Symbols;

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

extern "C" void* UdfAllocateWithSize(ui64 size) {
    return Symbols.UdfAllocateWithSizeFunc(size);
}

extern "C" void UdfFreeWithSize(const void* mem, ui64 size) {
    return Symbols.UdfFreeWithSizeFunc(mem, size);
}

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

namespace NYql::NUdf {

namespace {

using TBackTraceCallback = void (*)();
TBackTraceCallback BackTraceCallback;

void UdfBackTraceFn(IOutputStream*, void* const*, size_t) {
    BackTraceCallback();
}

} // namespace

void SetBackTraceCallbackImpl(TBackTraceCallback callback) {
    BackTraceCallback = callback;
    SetFormatBackTraceFn(UdfBackTraceFn);
}

} // namespace NYql::NUdf
