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
    auto keyLiteral = ToYPathLiteral(key);
    PushLiteral(std::move(keyLiteral));
}

void TYPathStack::PushLiteral(std::string key)
{
    if (PathMaterialized_) {
        PreviousPathLengths_.push_back(Path_.size());
        Path_.reserve(key.length() + 1);
        Path_ += "/";
        Path_ += key;
    }
    Items_.push_back(std::move(key));
}

void TYPathStack::Push(int index)
{
    if (PathMaterialized_) {
        PreviousPathLengths_.push_back(Path_.size());
        Path_ += "/";
        Path_ += std::to_string(index);
    }
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
    if (PathMaterialized_) {
        Path_.resize(PreviousPathLengths_.back());
        PreviousPathLengths_.pop_back();
    }
}

bool TYPathStack::IsEmpty() const
{
    return Items_.empty();
}

const TYPath& TYPathStack::GetPath() const
{
    if (!PathMaterialized_) {
        PreviousPathLengths_.reserve(Items_.size());
        Path_.reserve(Items_.size() * 2);
        for (const auto& entry : Items_) {
            PreviousPathLengths_.push_back(Path_.size());
            Path_ += "/";
            Visit(entry,
                [&] (const std::string& value) {
                    Path_ += value;
                },
                [&] (int value) {
                    Path_ += std::to_string(value);
                });
        }
        PathMaterialized_ = true;
    }
    return Path_;
}

std::string TYPathStack::GetHumanReadablePath() const
{
    auto path = GetPath();
    if (path.empty()) {
        static const std::string Root("(root)");
        return Root;
    }
    return path;
}

std::optional<std::string> TYPathStack::TryGetStringifiedLastPathToken() const
{
    if (Items_.empty()) {
        return {};
    }
    return ToString(Items_.back());
}

std::string TYPathStack::ToString(const TYPathStack::TEntry& item)
{
    return Visit(item,
        [&] (const std::string& string) {
            return string;
        },
        [&] (int integer) -> std::string {
            return std::to_string(integer);
        });
}

void TYPathStack::Reset()
{
    PreviousPathLengths_.clear();
    Path_.clear();
    Items_.clear();
    PathMaterialized_ = false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYPath
