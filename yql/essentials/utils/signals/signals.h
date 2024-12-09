#pragma once

#include <util/system/defaults.h>
#include <util/system/sigset.h>
#include <util/system/pipe.h>

#include <signal.h>


namespace NYql {

#ifdef _win_
using sig_atomic_t = int;
#endif

extern volatile sig_atomic_t NeedTerminate;
extern volatile sig_atomic_t NeedQuit;
extern volatile sig_atomic_t NeedReconfigure;
extern volatile sig_atomic_t NeedReopenLog;
extern volatile sig_atomic_t NeedReapZombies;
extern volatile sig_atomic_t NeedInterrupt;

extern TPipe SignalPipeW;
extern TPipe SignalPipeR;

void InitSignals();
void InitSignalsWithSelfPipe();
void CatchInterruptSignal(bool doCatch);

void SigSuspend(const sigset_t* mask);
void AllowAnySignals();
bool HasPendingQuitOrTerm();

} // namespace NYql
