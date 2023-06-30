#pragma once

#include <yt/yt/core/concurrency/coroutine.h>

#include <util/generic/strbuf.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//
// Class launches function processing pull-based stream in coroutine
// and allows to feed it with data in push-based manner.
class TCoroPipe
{
public:
    TCoroPipe(std::function<void(IZeroCopyInput*)> streamReader);

    void Feed(TStringBuf data);
    void Finish();

private:
    using TCoroutine = NConcurrency::TCoroutine<void(TStringBuf)>;
    TCoroutine Coroutine_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
