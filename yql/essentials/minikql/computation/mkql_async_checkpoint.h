#pragma once

#include <util/generic/string.h>
#include <util/generic/strbuf.h>

namespace NKikimr::NMiniKQL {

// Owner-only bridge to graph state. Implementations must not perform I/O while
// saving/parsing state. The engine owns input cuts, downstream acknowledgements
// and durable storage; the graph owns serializable operator values.
class IAsyncCheckpoint {
public:
    virtual ~IAsyncCheckpoint() = default;
    virtual TString BeforeSave() = 0;
    virtual void CheckSnapshotSize(ui64 bytes) const = 0;
    virtual void AfterSave() = 0;
    virtual void AbortSave() noexcept = 0;
    virtual void BeforeLoad(TStringBuf header, ui64 bytes) = 0;
    virtual void AfterLoad() = 0;
};

} // namespace NKikimr::NMiniKQL
