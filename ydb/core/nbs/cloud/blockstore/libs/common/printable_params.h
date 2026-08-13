#pragma once

#include "block_range.h"

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/stream/output.h>
#include <util/string/builder.h>
#include <util/system/types.h>
#include <util/system/yassert.h>

#include <concepts>
#include <cstring>
#include <span>
#include <type_traits>
#include <utility>
#include <variant>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// Type-erased printable value for types that are not listed in TPrintableValue.
// Stores the value inline; formatting happens only when streamed.
class TInlinePrintable
{
    static constexpr size_t MaxValueSize = 16;

public:
    TInlinePrintable() = default;

    // Streamability is checked when the printer lambda is instantiated; a
    // requires(out << v) clause here would recurse through this converting
    // constructor via ADL while TInlinePrintable is itself a variant
    // alternative.
    template <typename T>
        requires std::is_trivially_copyable_v<T> &&
                 (sizeof(T) <= MaxValueSize) && (alignof(T) <= 8) &&
                 (!std::convertible_to<const T&, TStringBuf>) &&
                 (!std::same_as<std::remove_cvref_t<T>, TString>)
    TInlinePrintable(const T& value)
        : Printer(+[](IOutputStream& out, const void* storage)
                  { out << *static_cast<const T*>(storage); })
    {
        std::memcpy(Storage, &value, sizeof(T));
    }

private:
    void (*Printer)(IOutputStream&, const void*) = nullptr;
    alignas(8) char Storage[MaxValueSize] = {};

    friend IOutputStream& operator<<(
        IOutputStream& out,
        const TInlinePrintable& value);
};

inline IOutputStream& operator<<(
    IOutputStream& out,
    const TInlinePrintable& value)
{
    Y_ABORT_UNLESS(value.Printer);
    value.Printer(out, value.Storage);
    return out;
}

////////////////////////////////////////////////////////////////////////////////

using TPrintableValue = std::variant<
    std::monostate,
    TString,
    int,
    ui16,
    ui32,
    ui64,
    TBlockRange64,
    TStringBuf,
    const char*,
    TInlinePrintable>;

using TPrintableParam = std::pair<TStringBuf, TPrintableValue>;
using TPrintableParams = std::span<const TPrintableParam>;

void PrintParams(IOutputStream& out, TPrintableParams keyValues);
TString PrintParams(TPrintableParams keyValues);

}   // namespace NYdb::NBS::NBlockStore
