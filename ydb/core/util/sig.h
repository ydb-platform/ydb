#pragma once

#include <util/system/sigset.h>
#include <util/generic/yexception.h>

static inline void ThreadSigmask(int how) {
    sigset_t Mask;
    SigEmptySet(&Mask);
    SigAddSet(&Mask, SIGINT);
    SigAddSet(&Mask, SIGTERM);
#ifndef _win_
    SigAddSet(&Mask, SIGUSR1);
    SigAddSet(&Mask, SIGUSR2);
    SigAddSet(&Mask, SIGHUP);
    SigAddSet(&Mask, SIGURG);
#endif
    if (SigProcMask(how, &Mask, nullptr))
        ythrow yexception() << "Cannot change signal mask";
}

