#pragma once

#include <util/generic/noncopyable.h>
#include <util/system/compiler.h>

#include <yql/essentials/public/issue/yql_issue.h>

namespace NKikimr::NMiniKQL {

class TTerminateException: public NYql::TErrorException {
public:
    TTerminateException();
};

///////////////////////////////////////////////////////////////////////////////
// ITerminator
///////////////////////////////////////////////////////////////////////////////
class ITerminator {
public:
    virtual ~ITerminator() = default;
    virtual void Terminate(const char* message) const = 0;
};

struct TBindTerminator: private TNonCopyable {
    explicit TBindTerminator(ITerminator* terminator);
    ~TBindTerminator();

    static thread_local ITerminator* Terminator;

private:
    ITerminator* PreviousTerminator_;
};

struct TThrowingBindTerminator: public TBindTerminator, public ITerminator {
    TThrowingBindTerminator();
    void Terminate(const char* message) const final;
};

struct TOnlyThrowingBindTerminator: public TBindTerminator, public ITerminator {
    TOnlyThrowingBindTerminator();
    void Terminate(const char* message) const final;
};

[[noreturn]] void MKQLTerminate(const char* message);

} // namespace NKikimr::NMiniKQL
