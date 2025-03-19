#pragma once

#include <string>

namespace NYdb {
inline namespace Dev {
namespace NUtils {

char* ToLower(char* str) noexcept(noexcept(std::tolower(0)));
std::string ToLower(const std::string& str);
void ToLower(std::string& str);

std::string ToTitle(const std::string& s);

void RemoveAll(std::string& str, char ch);

bool TrySplitOn(std::string_view src, std::string_view& l, std::string_view& r, size_t pos, size_t len);

bool TrySplit(std::string_view src, std::string_view& l, std::string_view& r, char delim);
bool TrySplit(std::string_view src, std::string_view& l, std::string_view& r, std::string_view delim);
bool TryRSplit(std::string_view src, std::string_view& l, std::string_view& r, char delim);
bool TryRSplit(std::string_view src, std::string_view& l, std::string_view& r, std::string_view delim);

void Split(std::string_view src, std::string_view& l, std::string_view& r, char delim);
void Split(std::string_view src, std::string_view& l, std::string_view& r, std::string_view delim);
void RSplit(std::string_view src, std::string_view& l, std::string_view& r, char delim);
void RSplit(std::string_view src, std::string_view& l, std::string_view& r, std::string_view delim);

std::string_view NextTok(std::string_view& src, char delim);
std::string_view NextTok(std::string_view& src, std::string_view delim);
bool NextTok(std::string_view& src, std::string_view& tok, char delim);
bool NextTok(std::string_view& src, std::string_view& tok, std::string_view delim);

std::string_view RNextTok(std::string_view& src, char delim);
std::string_view RNextTok(std::string_view& src, std::string_view delim);

std::string_view After(std::string_view src, char c);
std::string_view Before(std::string_view src, char c);

size_t SumLength() noexcept;

template <typename... R>
size_t SumLength(const std::string_view s1, const R&... r) noexcept {
    return s1.size() + SumLength(r...);
}

/**
    * Appends `src` to string dst of size `dsize` (unlike `strncat`, `dsize` is
    * the full size of `dst`, not space left). At most `dsize-1` characters
    * will be copied. Always NUL terminates (unless `dsize <= strlen(dst)`).
    * 
    * @return strlen(src) + MIN(dsize, strlen(initial dst));
    *         if retval >= dsize, truncation occurred.
    */
size_t Strlcat(char* dst, const char* src, size_t dsize) noexcept;

/**
    * Copy string `src` to buffer `dst` of size `dsize`. At most `dsize-1`
    * chars will be copied. Always NUL terminates (unless `dsize == 0`).
    * 
    * @return strlen(src); if retval >= dsize, truncation occurred.
    */
size_t Strlcpy(char* dst, const char* src, size_t dsize) noexcept;

void CopyAll(char*) noexcept;

template <typename... R>
void CopyAll(char* p, const std::string_view s, const R&... r) {
    std::string::traits_type::copy(p, s.data(), s.size());
    CopyAll(p + s.size(), r...);
}

template <typename... R>
std::string Join(const R&... r) {
    std::string s(SumLength(r...), '\0');

    CopyAll(s.data(), r...);

    return s;
}

template <typename TChar>
std::basic_string<TChar> FromAscii(const std::string_view& s);

}
}
}
