// Copyright 2024 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "core/fxcrt/win/win_util.h"

#include <windows.h>
#include <processthreadsapi.h>

namespace pdfium {

bool IsUser32AndGdi32Available() {
  static auto is_user32_and_gdi32_available = []() {
    // If win32k syscalls aren't disabled, then user32 and gdi32 are available.

    typedef decltype(
        GetProcessMitigationPolicy)* GetProcessMitigationPolicyType;
    GetProcessMitigationPolicyType get_process_mitigation_policy_func =
        reinterpret_cast<GetProcessMitigationPolicyType>(GetProcAddress(
            GetModuleHandle(L"kernel32.dll"), "GetProcessMitigationPolicy"));

    if (!get_process_mitigation_policy_func)
      return true;

    PROCESS_MITIGATION_SYSTEM_CALL_DISABLE_POLICY policy = {};
    if (get_process_mitigation_policy_func(GetCurrentProcess(),
                                           ProcessSystemCallDisablePolicy,
                                           &policy, sizeof(policy))) {
      return policy.DisallowWin32kSystemCalls == 0;
    }

    return true;
  }();
  return is_user32_and_gdi32_available;
}

}  // namespace pdfium
