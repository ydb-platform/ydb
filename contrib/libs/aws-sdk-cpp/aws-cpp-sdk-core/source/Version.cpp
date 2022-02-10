/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/core/Version.h>
#include <aws/core/VersionConfig.h>

namespace Aws
{
namespace Version
{
  const char* GetVersionString()
  {
    return AWS_SDK_VERSION_STRING;
  }

  unsigned GetVersionMajor()
  {
    return AWS_SDK_VERSION_MAJOR;
  }

  unsigned GetVersionMinor()
  {
    return AWS_SDK_VERSION_MINOR;
  }

  unsigned GetVersionPatch()
  {
    return AWS_SDK_VERSION_PATCH;
  }


  const char* GetCompilerVersionString()
  {
#define xstr(s) str(s)
#define str(s) #s
#if defined(_MSC_VER)
      return "MSVC/" xstr(_MSC_VER);
#elif defined(__clang__)
      return "Clang/" xstr(__clang_major__) "."  xstr(__clang_minor__) "." xstr(__clang_patchlevel__);
#elif defined(__GNUC__)
      return "GCC/" xstr(__GNUC__) "."  xstr(__GNUC_MINOR__) "." xstr(__GNUC_PATCHLEVEL__);
#else
      return "UnknownCompiler";
#endif
#undef str
#undef xstr
  }
} //namespace Version
} //namespace Aws


