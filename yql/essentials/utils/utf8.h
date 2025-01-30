#pragma once

#include <optional>
#include <string_view>

namespace NYql {

bool IsUtf8(const std::string_view& str);

unsigned char WideCharSize(char head);

std::optional<std::string> RoundToNearestValidUtf8(const std::string_view& str, bool roundDown);
std::optional<std::string> NextValidUtf8(const std::string_view& str);
std::optional<std::string> NextLexicographicString(const std::string_view& str);

}
