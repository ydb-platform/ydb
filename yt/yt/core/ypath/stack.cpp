#include "stack.h"

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/ypath/token.h>

#include <library/cpp/yt/misc/variant.h>
#include <library/cpp/yt/misc/cast.h>

#include <library/cpp/yt/string/string.h>

namespace NYT::NYPath {

////////////////////////////////////////////////////////////////////////////////

void TYPathStack::Push(TStringBuf key)
{
    PreviousPathLengths_.push_back(Path_.size());
    Path_ += "/";
    Path_ += ToYPathLiteral(key);
    Items_.emplace_back(TString(key));
}

void TYPathStack::Push(int index)
{
    PreviousPathLengths_.push_back(Path_.size());
    Path_ += "/";
    Path_ += ToString(index);
    Items_.emplace_back(index);
}

void TYPathStack::IncreaseLastIndex()
{
    YT_VERIFY(!Items_.empty());
    YT_VERIFY(std::holds_alternative<int>(Items_.back()));
    auto index = std::get<int>(Items_.back());
    Pop();
    ++index;
    Push(index);
}

void TYPathStack::Pop()
{
    YT_VERIFY(!Items_.empty());
    Items_.pop_back();
    Path_.resize(PreviousPathLengths_.back());
    PreviousPathLengths_.pop_back();
}

bool TYPathStack::IsEmpty() const
{
    return Items_.empty();
}

const TYPath& TYPathStack::GetPath() const
{
    return Path_;
}

TString TYPathStack::GetHumanReadablePath() const
{
    auto path = GetPath();
    if (path.empty()) {
        static const TString Root("(root)");
        return Root;
    }
    return path;
}

std::optional<TString> TYPathStack::TryGetStringifiedLastPathToken() const
{
    if (Items_.empty()) {
        return {};
    }
    return ToString(Items_.back());
}

TString TYPathStack::ToString(const TYPathStack::TEntry& item)
{
    return Visit(item,
        [&] (const TString& string) {
            return string;
        },
        [&] (int integer) {
            return ::ToString(integer);
        });
}

void TYPathStack::Reset()
{
    PreviousPathLengths_.clear();
    Path_.clear();
    Items_.clear();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYPath
