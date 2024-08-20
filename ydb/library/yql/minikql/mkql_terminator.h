#pragma once

#include <util/generic/noncopyable.h>
#include <util/system/compiler.h>


namespace NKikimr {

namespace NMiniKQL {
///////////////////////////////////////////////////////////////////////////////
// ITerminator
///////////////////////////////////////////////////////////////////////////////
class ITerminator
{
public:
    virtual ~ITerminator() = default;
    virtual void Terminate(const char* message) const = 0;
};

struct TBindTerminator : private TNonCopyable {
    TBindTerminator(ITerminator* terminator);
    ~TBindTerminator();

    static thread_local ITerminator* Terminator;
private:
    ITerminator* PreviousTerminator;
};

struct TThrowingBindTerminator : public TBindTerminator, public ITerminator {
    TThrowingBindTerminator();
    void Terminate(const char* message) const final;
};

struct TOnlyThrowingBindTerminator : public TBindTerminator, public ITerminator {
    TOnlyThrowingBindTerminator();
    void Terminate(const char* message) const final;
};



[[noreturn]] void MKQLTerminate(const char* message);

}

}
