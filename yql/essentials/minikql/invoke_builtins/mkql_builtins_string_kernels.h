#pragma once

#include <yql/essentials/minikql/mkql_function_metadata.h>

namespace NKikimr::NMiniKQL {

void RegisterStringKernelEquals(TKernelFamilyBase& kernelFamily);
void RegisterStringKernelNotEquals(TKernelFamilyBase& kernelFamily);
void RegisterStringKernelLess(TKernelFamilyBase& kernelFamily);
void RegisterStringKernelLessOrEqual(TKernelFamilyBase& kernelFamily);
void RegisterStringKernelGreater(TKernelFamilyBase& kernelFamily);
void RegisterStringKernelGreaterOrEqual(TKernelFamilyBase& kernelFamily);

void RegisterStringKernelSize(TKernelFamilyBase& kernelFamily);
void RegisterStringKernelStartsWith(TKernelFamilyBase& kernelFamily);
void RegisterStringKernelEndsWith(TKernelFamilyBase& kernelFamily);
void RegisterStringKernelContains(TKernelFamilyBase& kernelFamily);

void RegisterSizeBuiltin(TKernelFamilyMap& kernelFamilyMap);
void RegisterWith(TKernelFamilyMap& kernelFamilyMap);
} // namespace NKikimr::NMiniKQL
