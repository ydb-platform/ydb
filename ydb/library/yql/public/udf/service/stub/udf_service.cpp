#include <util/system/yassert.h>
#include <ydb/library/yql/public/udf/udf_value.h>

extern "C" void* UdfAllocate(ui64) { Y_ABORT("Called UdfAllocate"); }
extern "C" void UdfFree(const void*) { Y_ABORT("Called UdfFree"); }
extern "C" void UdfTerminate(const char*) { Y_ABORT("Called UdfTerminate."); }
extern "C" void UdfRegisterObject(::NYql::NUdf::TBoxedValue*) { Y_ABORT("Called UdfRegisterObject"); }
extern "C" void UdfUnregisterObject(::NYql::NUdf::TBoxedValue*) { Y_ABORT("Called UdfUnregisterObject"); }
extern "C" void* UdfAllocateWithSize(ui64) { Y_ABORT("Called UdfAllocateWithSize"); }
extern "C" void UdfFreeWithSize(const void*, ui64) { Y_ABORT("Called UdfFreeWithSize"); }
extern "C" void* UdfArrowAllocate(ui64) { Y_ABORT("Called UdfArrowAllocate"); }
extern "C" void* UdfArrowReallocate(const void*, ui64, ui64) { Y_ABORT("Called UdfArrowReallocate"); }
extern "C" void UdfArrowFree(const void*, ui64) { Y_ABORT("Called UdfArrowFree"); }
