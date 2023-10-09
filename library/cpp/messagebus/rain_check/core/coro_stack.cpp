#include "coro_stack.h"

#include <util/generic/singleton.h>
#include <util/system/valgrind.h>

#include <cstdlib>
#include <stdio.h>

using namespace NRainCheck;
using namespace NRainCheck::NPrivate;

TCoroStack::TCoroStack(size_t size)
    : SizeValue(size)
{
    Y_ABORT_UNLESS(size % sizeof(ui32) == 0);
    Y_ABORT_UNLESS(size >= 0x1000);

    DataHolder.Reset(malloc(size));

    // register in valgrind

    *MagicNumberLocation() = MAGIC_NUMBER;

#if defined(WITH_VALGRIND)
    ValgrindStackId = VALGRIND_STACK_REGISTER(Data(), (char*)Data() + Size());
#endif
}

TCoroStack::~TCoroStack() {
#if defined(WITH_VALGRIND)
    VALGRIND_STACK_DEREGISTER(ValgrindStackId);
#endif

    VerifyNoStackOverflow();
}

void TCoroStack::FailStackOverflow() {
    static const char message[] = "stack overflow\n";
    fputs(message, stderr);
    abort();
}
