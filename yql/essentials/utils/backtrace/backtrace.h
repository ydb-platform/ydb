#pragma once

#include <util/system/types.h>
#include <util/generic/string.h>
#include <util/generic/hash.h>

#include <functional>

bool SetSignalHandler(int signo, void (*handler)(int));
class IOutputStream;

void EnableKikimrBacktraceFormat();

namespace NYql {

namespace NBacktrace {
const int Limit = 400;

void RegisterKikimrFatalActions();
void AddAfterFatalCallback(const std::function<void(int)>& after);
void AddBeforeFatalCallback(const std::function<void(int)>& before);
void EnableKikimrSymbolize();

void KikimrBackTrace();
void KikimrBackTraceFormatImpl(IOutputStream*);
void KikimrBacktraceFormatImpl(IOutputStream* out, void* const* stack, size_t stackSize);

void SetModulesMapping(const THashMap<TString, TString>& mapping);

TString Symbolize(const TString& input, const THashMap<TString, TString>& mapping);

} /* namespace Backtrace */

} /* namespace NYql */
