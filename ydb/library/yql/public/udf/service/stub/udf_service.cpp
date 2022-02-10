#include <util/system/yassert.h> 
#include <ydb/library/yql/public/udf/udf_value.h>
 
extern "C" void* UdfAllocate(ui64) { Y_FAIL("Called UdfAllocate"); } 
extern "C" void UdfFree(const void*) { Y_FAIL("Called UdfFree"); } 
extern "C" void UdfTerminate(const char*) { Y_FAIL("Called UdfTerminate."); } 
extern "C" void UdfRegisterObject(::NYql::NUdf::TBoxedValue*) { Y_FAIL("Called UdfRegisterObject"); } 
extern "C" void UdfUnregisterObject(::NYql::NUdf::TBoxedValue*) { Y_FAIL("Called UdfUnregisterObject"); } 
extern "C" void* UdfAllocateWithSize(ui64) { Y_FAIL("Called UdfAllocateWithSize"); }
extern "C" void UdfFreeWithSize(const void*, ui64) { Y_FAIL("Called UdfFreeWithSize"); } 
