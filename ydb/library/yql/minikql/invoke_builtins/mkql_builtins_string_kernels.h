#pragma once

#include <ydb/library/yql/minikql/mkql_function_metadata.h>

namespace NKikimr {
namespace NMiniKQL {

void RegisterStringKernelEquals(TKernelFamilyBase& kernelFamily);
void RegisterStringKernelNotEquals(TKernelFamilyBase& kernelFamily);
void RegisterStringKernelLess(TKernelFamilyBase& kernelFamily);
void RegisterStringKernelLessOrEqual(TKernelFamilyBase& kernelFamily);
void RegisterStringKernelGreater(TKernelFamilyBase& kernelFamily);
void RegisterStringKernelGreaterOrEqual(TKernelFamilyBase& kernelFamily);

void RegisterStringKernelSize(TKernelFamilyBase& kernelFamily);
void RegisterSizeBuiltin(TKernelFamilyMap& kernelFamilyMap);
}
}
