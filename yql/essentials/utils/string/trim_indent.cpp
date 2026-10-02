#include "trim_indent.h"

#include <util/generic/algorithm.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/ascii.h>
#include <util/string/builder.h>
#include <util/string/join.h>
#include <util/string/split.h>

#include <ranges>

namespace NYql {

namespace {

constexpr auto IndentWidth = [](TStringBuf line) -> size_t {
    auto it = FindIf(line, [](char x) { return x != ' '; });
    if (it == end(line)) {
        return line.size();
    }

    return std::distance(begin(line), it);
};

constexpr auto IsNotBlank = [](TStringBuf line) -> bool {
    return !AllOf(line, [](char x) { return x == ' '; });
};

bool IsBlank(TStringBuf line) {
    return !IsNotBlank(line);
}

} // namespace

TString TrimIndent(TStringBuf input) {
    TVector<TStringBuf> lines;

    TSetDelimiter<const char> delim("\n");
    TContainerConsumer<TVector<TStringBuf>> consumer(&lines);
    SplitString(begin(input), end(input), delim, consumer);

    auto indents =
        lines |
        std::ranges::views::filter(IsNotBlank) |
        std::ranges::views::transform(IndentWidth);

    size_t minIndent = 0;
    if (!std::ranges::empty(indents)) {
        minIndent = std::ranges::min(indents);
    }

    for (TStringBuf& line : lines) {
        line.Skip(minIndent);
    }

    if (!lines.empty() && IsBlank(lines.front())) {
        lines.erase(begin(lines));
    }

    if (!lines.empty() && IsBlank(lines.back())) {
        lines.pop_back();
    }

    return JoinSeq('\n', lines);
}

} // namespace NYql
