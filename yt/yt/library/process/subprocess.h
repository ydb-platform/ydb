#pragma once

#include "public.h"
#include "process.h"

#include <library/cpp/yt/memory/ref.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TSubprocessResult
{
    TSharedRef Output;
    TSharedRef Error;
    TError Status;
};

////////////////////////////////////////////////////////////////////////////////

class TSubprocess
{
public:
    explicit TSubprocess(TString path, bool copyEnv = true);

    static TSubprocess CreateCurrentProcessSpawner();

    void AddArgument(TStringBuf arg);
    void AddArguments(std::initializer_list<TStringBuf> args);

    TSubprocessResult Execute(
        const TSharedRef& input = TSharedRef::MakeEmpty(),
        TDuration timeout = TDuration::Max());

    void Kill(int signal);

    TString GetCommandLine() const;

    TProcessBasePtr GetProcess() const;

private:
    const TString Path_;

    const TProcessBasePtr Process_;
};

////////////////////////////////////////////////////////////////////////////////

void RunSubprocess(const std::vector<TString>& cmd);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
