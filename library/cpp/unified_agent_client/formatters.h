#pragma once

#include <util/generic/string.h>
#include <util/string/builder.h>
#include <util/folder/path.h>
#include <format>

// Formatter specialization for TString (Yandex's string type)
// This allows TString to be used directly in std::format without calling .c_str()
//
// Usage:
//   TString str = "hello";
//   auto formatted = std::format("Message: {}", str);
//
template <>
struct std::formatter<TString> : std::formatter<std::string_view> {
    auto format(const TString& str, std::format_context& ctx) const {
        return std::formatter<std::string_view>::format(
            std::string_view(str.data(), str.size()), ctx);
    }
};

// Formatter specialization for TStringBuf (Yandex's string_view-like type)
template <>
struct std::formatter<TStringBuf> : std::formatter<std::string_view> {
    auto format(const TStringBuf& str, std::format_context& ctx) const {
        return std::formatter<std::string_view>::format(
            std::string_view(str.data(), str.size()), ctx);
    }
};

// Formatter specialization for TStringBuilder
template <>
struct std::formatter<TStringBuilder> : std::formatter<TString> {
    auto format(const TStringBuilder& str, std::format_context& ctx) const {
        return std::formatter<TString>::format(str, ctx);
    }
};

// Formatter specialization for TFsPath (Yandex's filesystem path type)
// Safe: format() immediately copies data to output buffer before 'str' is destroyed (C++20 temporary lifetime rule)
template <>
struct std::formatter<TFsPath> : std::formatter<std::string_view> {
    auto format(const TFsPath& path, std::format_context& ctx) const {
        const TString str(path);
        return std::formatter<std::string_view>::format(
            std::string_view(str.data(), str.size()), ctx);
    }
};
