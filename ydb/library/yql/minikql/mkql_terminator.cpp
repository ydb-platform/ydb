#include "defs.h"
#include "mkql_terminator.h"

#include <util/string/builder.h>

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

TThrowingBindTerminator::TThrowingBindTerminator()
    : TBindTerminator(this)
{
}

void TThrowingBindTerminator::Terminate(const char* message) const {
    TStringBuf reason = (message ? TStringBuf(message) : TStringBuf("(unknown)"));
    TString fullMessage = TStringBuilder() <<
        "Terminate was called, reason(" << reason.size() << "): " << reason << Endl;
    ythrow yexception() << fullMessage;
}

TOnlyThrowingBindTerminator::TOnlyThrowingBindTerminator()
    : TBindTerminator(this)
{
}

void TOnlyThrowingBindTerminator::Terminate(const char* message) const {
    ythrow yexception() << message;
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
