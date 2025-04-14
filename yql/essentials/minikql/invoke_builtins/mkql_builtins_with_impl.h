#include <string_view>

namespace NKikimr::NMiniKQL {

namespace {

template<bool IgnoreCase>
bool CharEquals(char a, char b) {
    if constexpr (IgnoreCase) {
        return std::toupper(a) == std::toupper(b);
    } else {
        return a == b;
    }
}

} //namespace

template<bool IgnoreCase>
bool StringStartsWith(std::string_view input, std::string_view substring) {
    return substring.size() <= input.size() && std::equal(begin(substring), end(substring), begin(input), CharEquals<IgnoreCase>);
}
template<bool IgnoreCase>
bool StringEndsWith(std::string_view input, std::string_view substring) {
    return substring.size() <= input.size() && std::equal(begin(substring), end(substring), end(input) - substring.size(), CharEquals<IgnoreCase>);
}
template<bool IgnoreCase>
bool StringContainsSubstring(std::string_view input, std::string_view substring) {
    return std::search(begin(input), end(input), begin(substring), end(substring), CharEquals<IgnoreCase>) != end(input);
};

} //namespace NKikimr::NMiniKQL