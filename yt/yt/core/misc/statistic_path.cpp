#include "statistic_path.h"

#include "error.h"

#include <yt/yt/core/yson/format.h>

namespace NYT::NStatisticPath {

////////////////////////////////////////////////////////////////////////////////

TError CheckStatisticPathLiteral(const TStatisticPathType& literal)
{
    if (literal.Empty()) {
        return TError("Empty statistic path literal");
    }
    constexpr static TChar invalidCharacters[2]{Delimiter, 0};
    constexpr static TBasicStringBuf<TChar> invalidCharacterBuf(invalidCharacters, 2);
    if (auto invalidPos = literal.find_first_of(invalidCharacterBuf);
        invalidPos != TStatisticPathType::npos) {
        return TError("Invalid character found in a statistic path literal")
            << TErrorAttribute("literal", literal)
            << TErrorAttribute("invalid_character", static_cast<ui8>(literal[invalidPos]));
    }
    return {};
}

TErrorOr<TStatisticPathLiteral> ParseStatisticPathLiteral(const TStatisticPathType& literal)
{
    if (auto error = CheckStatisticPathLiteral(literal); !error.IsOK()) {
        return error;
    }
    return TStatisticPathLiteral(literal, false);
}

////////////////////////////////////////////////////////////////////////////////

TStatisticPathLiteral::TStatisticPathLiteral(const TStatisticPathType& literal, bool /*tag*/) noexcept
    : Literal_(literal)
{ }

TStatisticPathLiteral::TStatisticPathLiteral(const TStatisticPathType& literal)
    : TStatisticPathLiteral(ParseStatisticPathLiteral(literal).ValueOrThrow())
{ }

const TStatisticPathType& TStatisticPathLiteral::Literal() const noexcept
{
    return Literal_;
}

////////////////////////////////////////////////////////////////////////////////

TStatisticPath::TStatisticPath(const TStatisticPathType& path)
    : Path_(path)
{ }

TStatisticPath::TStatisticPath(TStatisticPathType&& path) noexcept
    : Path_(std::move(path))
{ }

TStatisticPath::TStatisticPath(const TStatisticPathLiteral& literal)
    : Path_(TStatisticPathType(Delimiter) + literal.Literal())
{ }

////////////////////////////////////////////////////////////////////////////////

TError CheckStatisticPath(const TStatisticPathType& path)
{
    constexpr static TChar twoDelimiters[2]{Delimiter, Delimiter};
    constexpr static TBasicStringBuf<TChar> adjacentDelimiters(twoDelimiters, 2);

    TError error;
    if (path.empty()) {
        return {};
    }

    if (path.front() != Delimiter) {
        error = TError("Statistic path must start with a delimiter");
    } else if (path.back() == Delimiter) {
        error = TError("Statistic path must not end with a delimiter");
    } else if (path.Contains(0)) {
        error = TError("Statistic path must not contain a null character");
    } else if (path.Contains(adjacentDelimiters)) {
        error = TError("Statistic path must not contain adjacent delimiters");
    }

    if (!error.IsOK()) {
        return error
            << TErrorAttribute("path", path);
    }
    return {};
}

TErrorOr<TStatisticPath> ParseStatisticPath(const TStatisticPathType& path)
{
    if (auto error = CheckStatisticPath(path); !error.IsOK()) {
        return error;
    }
    return TStatisticPath(path);
}

////////////////////////////////////////////////////////////////////////////////

const TStatisticPathType& TStatisticPath::Path() const noexcept
{
    return Path_;
}

////////////////////////////////////////////////////////////////////////////////

bool TStatisticPath::Empty() const noexcept
{
    return Path_.empty();
}

////////////////////////////////////////////////////////////////////////////////

void TStatisticPath::Swap(TStatisticPath& other) noexcept
{
    Path_.swap(other.Path_);
}

////////////////////////////////////////////////////////////////////////////////

TStatisticPath& TStatisticPath::Append(const TStatisticPathLiteral& literal)
{
    return Append(TStatisticPath(literal));
}

TStatisticPath& TStatisticPath::Append(const TStatisticPath& other)
{
    Path_.append(other.Path_);
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

TStatisticPath::const_iterator::value_type TStatisticPath::Front() const noexcept
{
    YT_VERIFY(!Empty());
    return *begin();
}

TStatisticPath::const_iterator::value_type TStatisticPath::Back() const noexcept
{
    YT_VERIFY(!Empty());
    return *(--end());
}

////////////////////////////////////////////////////////////////////////////////

void TStatisticPath::PopBack()
{
    YT_VERIFY(!Empty());
    Path_.resize(Path_.size() - Back().Size() - 1);
}

////////////////////////////////////////////////////////////////////////////////

bool TStatisticPath::StartsWith(const TStatisticPath& prefix) const
{
    // TODO(pavook) std::ranges::starts_with(*this, prefix) when C++23 arrives.
    auto [selfIt, prefixIt] = std::ranges::mismatch(*this, prefix);
    return prefixIt == prefix.end();
}

bool TStatisticPath::EndsWith(const TStatisticPath& suffix) const
{
    // TODO(pavook) std::ranges::ends_with(*this, suffix) when C++23 arrives.
    auto [selfIt, suffixIt] = std::mismatch(rbegin(), rend(), suffix.rbegin(), suffix.rend());
    return suffixIt == suffix.rend();
}

TStatisticPath operator/(const TStatisticPath& lhs, const TStatisticPathLiteral& rhs)
{
    return TStatisticPath(lhs).Append(rhs);
}

TStatisticPath operator/(const TStatisticPath& lhs, const TStatisticPath& rhs)
{
    return TStatisticPath(lhs).Append(rhs);
}

TStatisticPath operator/(TStatisticPath&& lhs, const TStatisticPathLiteral& rhs)
{
    return TStatisticPath(std::move(lhs)).Append(rhs);
}

TStatisticPath operator/(TStatisticPath&& lhs, const TStatisticPath& rhs)
{
    return TStatisticPath(std::move(lhs)).Append(rhs);
}

TStatisticPath operator/(const TStatisticPathLiteral& lhs, const TStatisticPathLiteral& rhs)
{
    return TStatisticPath(lhs).Append(rhs);
}

////////////////////////////////////////////////////////////////////////////////

bool operator==(const TStatisticPath& lhs, const TStatisticPath& rhs) noexcept
{
    return lhs.Path() == rhs.Path();
}

std::strong_ordering operator<=>(const TStatisticPath& lhs, const TStatisticPath& rhs) noexcept
{
    return lhs.Path().compare(rhs.Path()) <=> 0;
}

////////////////////////////////////////////////////////////////////////////////

namespace NStatisticPathLiterals {

TStatisticPathLiteral operator""_L(const char* str, size_t len)
{
    return TStatisticPathLiteral(TStatisticPathType(str, len));
}

} // namespace NStatisticPathLiterals

////////////////////////////////////////////////////////////////////////////////

TStatisticPath::TIterator::TIterator(const TStatisticPath& path, TStatisticPath::TIterator::TBeginIteratorTag) noexcept
    : Path_(path.Path())
    , Entry_(Path_.begin(), Path_.begin())
{
    Advance();
}

TStatisticPath::TIterator::TIterator(const TStatisticPath& path, TStatisticPath::TIterator::TEndIteratorTag) noexcept
    : Path_(path.Path())
    , Entry_()
{ }

////////////////////////////////////////////////////////////////////////////////

TStatisticPath::TIterator::reference TStatisticPath::TIterator::operator*() const noexcept
{
    return Entry_;
}

////////////////////////////////////////////////////////////////////////////////

TStatisticPath::TIterator& TStatisticPath::TIterator::operator++() noexcept
{
    Advance();
    return *this;
}

TStatisticPath::TIterator TStatisticPath::TIterator::operator++(int) noexcept
{
    TIterator result = *this;
    Advance();
    return result;
}

TStatisticPath::TIterator& TStatisticPath::TIterator::operator--() noexcept
{
    Retreat();
    return *this;
}

TStatisticPath::TIterator TStatisticPath::TIterator::operator--(int) noexcept
{
    TIterator result = *this;
    Retreat();
    return result;
}

////////////////////////////////////////////////////////////////////////////////

void TStatisticPath::TIterator::Advance() noexcept
{
    if (Entry_.end() == Path_.end()) {
        Entry_ = value_type();
    } else {
        Entry_ = value_type(std::next(Entry_.end()), Path_.end()).NextTok(Delimiter);
    }
}

void TStatisticPath::TIterator::Retreat() noexcept
{
    if (!Entry_) {
        Entry_ = value_type(Path_.begin(), Path_.end()).RNextTok(Delimiter);
    } else {
        Entry_ = value_type(Path_.begin(), std::prev(Entry_.begin())).RNextTok(Delimiter);
    }
}

////////////////////////////////////////////////////////////////////////////////

bool operator==(const TStatisticPath::TIterator& lhs, const TStatisticPath::TIterator& rhs) noexcept
{
    // TODO(pavook) should we compare sizes?
    return lhs.Entry_.data() == rhs.Entry_.data();
}

////////////////////////////////////////////////////////////////////////////////

// TODO(pavook) make it constant-time to comply with ranges::range semantic requirements?
TStatisticPath::const_iterator TStatisticPath::begin() const noexcept
{
    return {*this, TStatisticPath::TIterator::TBeginIteratorTag{}};
}

TStatisticPath::const_iterator TStatisticPath::end() const noexcept
{
    return {*this, TStatisticPath::TIterator::TEndIteratorTag{}};
}

TStatisticPath::const_reverse_iterator TStatisticPath::rbegin() const noexcept
{
    return std::make_reverse_iterator(end());
}

TStatisticPath::const_reverse_iterator TStatisticPath::rend() const noexcept
{
    return std::make_reverse_iterator(begin());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NStatisticPath
