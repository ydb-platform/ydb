#pragma once

#include <util/generic/fwd.h>
#include <util/generic/list.h>

TString GetYdbEndpoint();
TString GetYdbDatabase();

TString RunYdb(const TList<TString>& args1, const TList<TString>& args2);

ui64 GetFullTimeValue(const TString& output);
