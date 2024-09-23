#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_terminator.h>

extern "C" void* UdfAllocate(ui64 size) {
    return ::NKikimr::NMiniKQL::MKQLAllocDeprecated(size, ::NKikimr::NMiniKQL::EMemorySubPool::Default);
}

extern "C" void UdfFree(const void* mem) {
    return ::NKikimr::NMiniKQL::MKQLFreeDeprecated(mem, ::NKikimr::NMiniKQL::EMemorySubPool::Default);
}

extern "C" [[noreturn]] void UdfTerminate(const char* message) {
    ::NKikimr::NMiniKQL::MKQLTerminate(message);
}

extern "C" void UdfRegisterObject(::NYql::NUdf::TBoxedValue* object) {
    return ::NKikimr::NMiniKQL::MKQLRegisterObject(object);
}

extern "C" void UdfUnregisterObject(::NYql::NUdf::TBoxedValue* object) {
    return ::NKikimr::NMiniKQL::MKQLUnregisterObject(object);
}

extern "C" void* UdfAllocateWithSize(ui64 size) {
    return ::NKikimr::NMiniKQL::TWithDefaultMiniKQLAlloc::AllocWithSize(size);
}

extern "C" void UdfFreeWithSize(const void* mem, ui64 size) {
    return ::NKikimr::NMiniKQL::TWithDefaultMiniKQLAlloc::FreeWithSize(mem, size);
}

extern "C" void* UdfArrowAllocate(ui64 size) { 
    return ::NKikimr::NMiniKQL::MKQLArrowAllocate(size);
}

extern "C" void* UdfArrowReallocate(const void* mem, ui64 prevSize, ui64 size) { 
    return ::NKikimr::NMiniKQL::MKQLArrowReallocate(mem, prevSize, size);
}

extern "C" void UdfArrowFree(const void* mem, ui64 size) {
    ::NKikimr::NMiniKQL::MKQLArrowFree(mem, size);
}
