#pragma once

#include "defs.h"

/////////////////////////////////////////////////////////////
// TSerializableAccessChecker is used for checking that
// methods of some class are never called from different threads
// in parallel. Usage pattern is the following:
//
// class T {
//     void f() {
//         auto m = Guard(Lock);
//         ... do something ...
//     }
// private:
//     TSerializableAccessChecker Lock;
// };
/////////////////////////////////////////////////////////////
struct TSerializableAccessChecker {
    TSerializableAccessChecker()
        : Locked(0)
    {}

    void Acquire() {
        Y_ABORT_UNLESS(AtomicGet(Locked) == 0);
        AtomicSet(Locked, 1);
    }

    void Release() {
        Y_ABORT_UNLESS(AtomicGet(Locked) == 1);
        AtomicSet(Locked, 0);
    }
private:
    TAtomic Locked;
};
