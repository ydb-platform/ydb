#pragma once

#include "public.h"

#include <yt/yt/core/yson/public.h>
#include <yt/yt/core/ytree/public.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NYT {

namespace NStatisticPath {

////////////////////////////////////////////////////////////////////////////////

//! A type to store paths and literals as.
using TStatisticPathType = TString;

using TChar = TStatisticPathType::TChar;

//! Delimiter character to use instead of `/`. We want it to compare "less" than any other character.
//! See YT-22118 for motivation.
static constexpr TChar Delimiter = '\x01';

////////////////////////////////////////////////////////////////////////////////

//! Checks that `literal` is a valid statistic path literal, i.e. `literal` isn't empty,
//! doesn't contain `Delimiter` and doesn't contain `\0`.
TError CheckStatisticPathLiteral(const TStatisticPathType& literal);

class TStatisticPathLiteral
{
private:
    TStatisticPathLiteral(const TStatisticPathType& literal, bool /*tag*/) noexcept;

    friend TErrorOr<TStatisticPathLiteral> ParseStatisticPathLiteral(const TStatisticPathType& literal);

public:
    //! Can throw an exception in case `literal` is invalid (see `CheckStatisticPathLiteral`).
    explicit TStatisticPathLiteral(const TStatisticPathType& literal);

    friend class TStatisticPath;

    [[nodiscard]] bool operator==(const TStatisticPathLiteral& other) const noexcept = default;

    //! Returns the underlying string.
    [[nodiscard]] const TStatisticPathType& Literal() const noexcept;

private:
    TStatisticPathType Literal_;
};

//! Checks that `literal` is a valid statistic path literal (see `CheckStatisticPathLiteral`) and
//! returns TStatisticPathLiteral if it is. Returns an error with explanation otherwise.
TErrorOr<TStatisticPathLiteral> ParseStatisticPathLiteral(const TStatisticPathType& literal);

////////////////////////////////////////////////////////////////////////////////

//! Checks that `path` is a valid statistic path (it can be constructed as a sequence of literals).
//! Returns an error with explanation otherwise.
TError CheckStatisticPath(const TStatisticPathType& path);

//! Checks that `path` is a valid statistic path (it can be constructed as a sequence of literals),
//! and returns a `TStatisticPath` built from it if it is. Returns an error with explanation otherwise.
TErrorOr<TStatisticPath> ParseStatisticPath(const TStatisticPathType& path);

// TODO(pavook) constexpr when constexpr std::string arrives.
class TStatisticPath
{
private:
    class TIterator
    {
    public:
        using iterator_category = std::bidirectional_iterator_tag;
        using value_type = TBasicStringBuf<TChar>;
        using difference_type = value_type::difference_type;
        using pointer = void;

        // This is needed for iterator sanity as we store the value inside the iterator.
        // This is allowed, as since C++20 ForwardIterator::reference doesn't have to be an actual reference.
        using reference = const value_type;

        reference operator*() const noexcept;

        TIterator& operator++() noexcept;

        TIterator operator++(int) noexcept;

        TIterator& operator--() noexcept;

        TIterator operator--(int) noexcept;

        friend bool operator==(const TIterator& lhs, const TIterator& rhs) noexcept;

        TIterator() noexcept = default;

        TIterator(const TIterator& other) noexcept = default;

        TIterator& operator=(const TIterator& other) noexcept = default;

    private:
        value_type Path_;
        value_type Entry_;

        struct TBeginIteratorTag
        { };

        struct TEndIteratorTag
        { };

        TIterator(const TStatisticPath& path, TBeginIteratorTag) noexcept;

        TIterator(const TStatisticPath& path, TEndIteratorTag) noexcept;

        void Advance() noexcept;

        void Retreat() noexcept;

        friend class TStatisticPath;
    };

    friend bool operator==(const TIterator& lhs, const TIterator& rhs) noexcept;

public:
    using iterator = TIterator;
    using const_iterator = iterator;
    using reverse_iterator = std::reverse_iterator<TIterator>;
    using const_reverse_iterator = reverse_iterator;

    //! Constructs an empty path. The underlying string will be empty.
    TStatisticPath() noexcept = default;

    //! Constructs a path from an existing literal. The literal will be prepended with a Delimiter.
    TStatisticPath(const TStatisticPathLiteral& literal);

    TStatisticPath(const TStatisticPath& other) = default;

    TStatisticPath(TStatisticPath&& other) noexcept = default;

    TStatisticPath& operator=(const TStatisticPath& other) = default;

    TStatisticPath& operator=(TStatisticPath&& other) noexcept = default;

    ~TStatisticPath() noexcept = default;

    void Swap(TStatisticPath& other) noexcept;

    //! Returns `true` if the path is empty.
    [[nodiscard]] bool Empty() const noexcept;

    //! Returns the underlying path string.
    [[nodiscard]] const TStatisticPathType& Path() const noexcept;

    //! Appends a literal to the end of the path, adding a Delimiter between them.
    TStatisticPath& Append(const TStatisticPathLiteral& literal);

    //! Appends another path to the end of the current path. Essentially concatenates the underlying strings.
    TStatisticPath& Append(const TStatisticPath& other);

    [[nodiscard]] const_iterator begin() const noexcept;

    [[nodiscard]] const_iterator end() const noexcept;

    [[nodiscard]] const_reverse_iterator rbegin() const noexcept;

    [[nodiscard]] const_reverse_iterator rend() const noexcept;

    //! Returns the first literal of the path. Verifies that the path is not empty.
    [[nodiscard]] const_iterator::value_type Front() const noexcept;

    //! Returns the last literal of the path. Verifies that the path is not empty.
    [[nodiscard]] const_iterator::value_type Back() const noexcept;

    //! Removes the last literal from the path. Verifies that the path is not empty.
    void PopBack();

    //! Returns `true` if `path` is a prefix of the current path.
    [[nodiscard]] bool StartsWith(const TStatisticPath& path) const;

    //! Returns `true` if `path` is a suffix of the current path.
    [[nodiscard]] bool EndsWith(const TStatisticPath& path) const;

private:
    TStatisticPathType Path_;

    TStatisticPath& AppendStr(const TStatisticPathType& path);

    //! Private, build the path from literals instead.
    explicit TStatisticPath(const TStatisticPathType& path);

    //! Private, build the path from literals instead.
    explicit TStatisticPath(TStatisticPathType&& path) noexcept;

    friend struct TStatisticPathSerializer;

    friend TErrorOr<TStatisticPath> ParseStatisticPath(const TStatisticPathType& path);
};

// TODO(pavook) `= default` when it stops crashing the compiler.
[[nodiscard]] bool operator==(const TStatisticPath& lhs, const TStatisticPath& rhs) noexcept;

[[nodiscard]] std::strong_ordering operator<=>(const TStatisticPath& lhs, const TStatisticPath& rhs) noexcept;

////////////////////////////////////////////////////////////////////////////////

//! Appends `rhs` to `lhs`. See `TStatisticPath::Append`.
[[nodiscard]] TStatisticPath operator/(const TStatisticPath& lhs, const TStatisticPathLiteral& rhs);

//! Appends `rhs` to `lhs`. See `TStatisticPath::Append`.
[[nodiscard]] TStatisticPath operator/(const TStatisticPath& lhs, const TStatisticPath& rhs);

//! Appends `rhs` to `lhs`. See `TStatisticPath::Append`.
[[nodiscard]] TStatisticPath operator/(TStatisticPath&& lhs, const TStatisticPathLiteral& rhs);

//! Appends `rhs` to `lhs`. See `TStatisticPath::Append`.
[[nodiscard]] TStatisticPath operator/(TStatisticPath&& lhs, const TStatisticPath& rhs);

//! Appends `rhs` to `lhs`. See `TStatisticPath::Append`.
[[nodiscard]] TStatisticPath operator/(const TStatisticPathLiteral& lhs, const TStatisticPathLiteral& rhs);

////////////////////////////////////////////////////////////////////////////////

namespace NStatisticPathLiterals {

[[nodiscard]] TStatisticPathLiteral operator""_L(const char* str, size_t len);

} // namespace NStatisticPathLiterals

////////////////////////////////////////////////////////////////////////////////

struct TStatisticPathSerializer
{
    template <class C>
    static void Save(C& context, const NStatisticPath::TStatisticPath& path);

    template <class C>
    static void Load(C& context, NStatisticPath::TStatisticPath& path);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NStatisticPath

template <class C>
struct TSerializerTraits<NStatisticPath::TStatisticPath, C, void>
{
    using TSerializer = NStatisticPath::TStatisticPathSerializer;
    using TComparer = TValueBoundComparer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define STATISTIC_PATH_INL_H_
#include "statistic_path-inl.h"
#undef STATISTIC_PATH_INL_H_
