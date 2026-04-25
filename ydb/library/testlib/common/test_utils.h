#pragma once

#include <ydb/library/actors/core/actorsystem_fwd.h>

#include <util/datetime/base.h>

namespace NTestUtils {

// Wait until predicate is true or timeout is reached 
void WaitFor(TDuration timeout, const TString& description, std::function<bool(TString& info)> predicate);
void WaitFor(TDuration timeout, const TString& description, std::function<bool()> predicate);

void RestartTablet(const NActors::TActorSystem& runtime, ui64 tabletId);

}  // namespace NTestUtils
