#pragma once

#include "public.h"

#include <util/generic/hash.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>

#include <span>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////
// This class is similar to TStringBuf but there is a difference:
// it allows to have Start == nullptr and Length != 0 while such
// behaviour is prohibited for std::basic_string_view and it's derivatives
// The purpose if such class in NBS is to express ZeroBlocks(Trim) where
// we have some range of "no data"

class TBlockDataRef
{
private:
    const char* Start;
    size_t Length;

private:
    constexpr inline TBlockDataRef(std::nullptr_t, size_t size)
        : Start(nullptr)
        , Length(size)
    {}

public:
    constexpr inline TBlockDataRef(const char* data, size_t len) noexcept
        : Start(data)
        , Length(len)
    {
        Y_ABORT_UNLESS(data && len);
    }

    constexpr inline TBlockDataRef() noexcept
        : Start(nullptr)
        , Length(0)
    {}

    static TBlockDataRef Create(const TVector<ui8>& data)
    {
        return {reinterpret_cast<const char*>(data.data()), data.size()};
    }

    static TBlockDataRef CreateZeroBlock(size_t len)
    {
        return {nullptr, len};
    }

    TStringBuf AsStringBuf() const
    {
        if (Start == nullptr) {
            return {};
        }
        return TStringBuf(Start, Length);
    }

    [[nodiscard]] constexpr const char* Data() const noexcept
    {
        return Start;
    }

    [[nodiscard]] constexpr inline size_t Size() const noexcept
    {
        return Length;
    }

    Y_PURE_FUNCTION
    [[nodiscard]] constexpr inline bool Empty() const noexcept
    {
        return (Start == nullptr) || (Length == 0);
    }

    // Lowercase methods are useful for template functions that use TString and
    // TBlockDataRef.
    [[nodiscard]] constexpr const char* data() const noexcept
    {
        return Data();
    }

    [[nodiscard]] constexpr inline size_t size() const noexcept
    {
        return Size();
    }

    Y_PURE_FUNCTION
    [[nodiscard]] constexpr inline bool empty() const noexcept
    {
        return Empty();
    }

    constexpr inline explicit operator bool() const noexcept
    {
        return !Empty();
    }

    bool operator==(TBlockDataRef b) const
    {
        if (Start == nullptr) {
            return (b.Data() == nullptr) && (Size() == b.Size());
        }
        return AsStringBuf() == b.AsStringBuf();
    }

    inline size_t hash() const noexcept
    {
        if (Start == nullptr) {
            return ComputeHash(TStringBuf());
        }
        return ComputeHash(TStringBuf(Start, Length));
    }
};

using TBlockDataRefSpan = std::span<const TBlockDataRef>;

}   // namespace NYdb::NBS

template <>
inline void Out<NYdb::NBS::TBlockDataRef>(
    IOutputStream& o,
    const NYdb::NBS::TBlockDataRef& p)
{
    if (!p.Empty()) {
        o.Write(p.Data(), p.Size());
    }
}
