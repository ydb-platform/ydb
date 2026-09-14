#include "defs.h"
#include "mkql_terminator.h"

#include <yql/essentials/core/issue/yql_issue.h>

#include <util/string/builder.h>

namespace NKikimr::NMiniKQL {

TTerminateException::TTerminateException()
    : TErrorException(NYql::EYqlIssueCode::TIssuesIds_EIssueCode_CORE_RUNTIME_ERROR)
{
}

thread_local ITerminator* TBindTerminator::Terminator = nullptr;

TBindTerminator::TBindTerminator(ITerminator* terminator)
    : PreviousTerminator_(Terminator)
{
    Terminator = terminator;
}

TBindTerminator::~TBindTerminator()
{
    Terminator = PreviousTerminator_;
}

TThrowingBindTerminator::TThrowingBindTerminator()
    : TBindTerminator(this)
{
}

void TThrowingBindTerminator::Terminate(const char* message) const {
    TStringBuf reason = (message ? TStringBuf(message) : TStringBuf("(unknown)"));
    TString fullMessage = TStringBuilder() << "Terminate was called, reason(" << reason.size() << "): " << reason << Endl;
    ythrow TTerminateException() << fullMessage;
}

TOnlyThrowingBindTerminator::TOnlyThrowingBindTerminator()
    : TBindTerminator(this)
{
}

void TOnlyThrowingBindTerminator::Terminate(const char* message) const {
    ythrow TTerminateException() << message;
}

[[noreturn]] void MKQLTerminate(const char* message) {
    if (const auto t = TBindTerminator::Terminator) {
        t->Terminate(message);
    }

    if (message) {
        Cerr << message << Endl;
    }
    ::abort();
}

} // namespace NKikimr::NMiniKQL
