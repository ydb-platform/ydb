#include "defs.h"
#include "mkql_terminator.h"

namespace NKikimr {

namespace NMiniKQL {

thread_local ITerminator* TBindTerminator::Terminator = nullptr;

TBindTerminator::TBindTerminator(ITerminator* terminator)
    : PreviousTerminator(Terminator)
{
    Terminator = terminator;
}

TBindTerminator::~TBindTerminator()
{
    Terminator = PreviousTerminator;
}

[[noreturn]] void MKQLTerminate(const char* message) { 
    if (const auto t = TBindTerminator::Terminator)
        t->Terminate(message);

    if (message)
        Cerr << message << Endl;
    ::abort();
}

}

}
