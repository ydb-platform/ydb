#pragma once

#include <util/datetime/base.h>

namespace NTestUtils {

// Wait until predicate is true or timeout is reached 
void WaitFor(TDuration timeout, const TString& description, std::function<bool(TString& info)> predicate);
void WaitFor(TDuration timeout, const TString& description, std::function<bool()> predicate);

}  // namespace NTestUtils
