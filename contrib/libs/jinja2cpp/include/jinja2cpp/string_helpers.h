#ifndef JINJA2CPP_STRING_HELPERS_H
#define JINJA2CPP_STRING_HELPERS_H

#include "value.h"

#include <string_view>

#include <cwchar>
#include <string>
#include <type_traits>

namespace jinja2
{
namespace detail
{
    template<typename Src, typename Dst>
    struct StringConverter;

    template<typename Src>
    struct StringConverter<Src, Src>
    {
        static Src DoConvert(const std::basic_string_view<typename Src::value_type>& from)
        {
            return Src(from.begin(), from.end());
        }
    };

    template<>
    struct StringConverter<std::wstring, std::string>
    {
        static std::string DoConvert(const std::wstring_view& from)
        {
            std::mbstate_t state = std::mbstate_t();
            auto srcPtr = from.data();
            std::size_t srcSize = from.size();
            std::size_t destBytes = 0;

#ifndef _MSC_VER
            destBytes = std::wcsrtombs(nullptr, &srcPtr, srcSize, &state);
            if (destBytes == static_cast<std::size_t>(-1))
                return std::string();
#else
            auto err = wcsrtombs_s(&destBytes, nullptr, 0, &srcPtr, srcSize, &state);
            if (err != 0)
                return std::string();
#endif
            std::string result;
#ifndef _MSC_VER
            result.resize(destBytes + 1);
            auto converted = std::wcsrtombs(&result[0], &srcPtr, srcSize, &state);
            if (converted == static_cast<std::size_t>(-1))
                return std::string();
            result.resize(converted);
#else
            result.resize(destBytes);
            wcsrtombs_s(&destBytes, &result[0], destBytes, &srcPtr, srcSize, &state);
            result.resize(destBytes - 1);
#endif
            return result;
        }
    };

    template<>
    struct StringConverter<std::string, std::wstring>
    {
        static std::wstring DoConvert(const std::string_view& from)
        {
            std::mbstate_t state = std::mbstate_t();
            auto srcPtr = from.data();
            std::size_t srcSize = from.size();
            std::size_t destBytes = 0;

#ifndef _MSC_VER
            destBytes = std::mbsrtowcs(nullptr, &srcPtr, srcSize, &state);
            if (destBytes == static_cast<std::size_t>(-1))
                return std::wstring();
#else
            auto err = mbsrtowcs_s(&destBytes, nullptr, 0, &srcPtr, srcSize, &state);
            if (err != 0)
                return std::wstring();
#endif
            std::wstring result;
#ifndef _MSC_VER
            result.resize(destBytes + 1);
            srcPtr = from.data();
            auto converted = std::mbsrtowcs(&result[0], &srcPtr, srcSize, &state);
            if (converted == static_cast<std::size_t>(-1))
                return std::wstring();
            result.resize(converted);
#else
            result.resize(destBytes);
            mbsrtowcs_s(&destBytes, &result[0], destBytes, &srcPtr, srcSize, &state);
            result.resize(destBytes - 1);
#endif
            return result;
        }
    };

    template<typename CharT, typename T>
    struct StringConverter<std::basic_string_view<CharT>, T> : public StringConverter<std::basic_string<CharT>, T> {};

} // namespace detail

/*!
 * \brief Convert string objects from one representation or another
 *
 * Converts string or string views to string objects with possible char conversion (char -> wchar_t or wchar_t -> char).
 * This function should be used when exact type of string is needed.
 *
 * @tparam Dst Destination string type. Mandatory. Can be std::string or std::wstring
 * @tparam Src Source string type. Auto detected. Can be either std::basic_string<CharT> or std::string_view<CharT>
 *
 * @param from Source string object which should be converted
 * @return Destination string object of the specified type
 */
template<typename Dst, typename Src>
Dst ConvertString(Src&& from)
{
    using src_t = std::decay_t<Src>;
    return detail::StringConverter<src_t, std::decay_t<Dst>>::DoConvert(std::basic_string_view<typename src_t::value_type>(from));
}

/*!
 * \brief Gets std::string from std::string
 *
 * Helper method for use in template context which gets std::string from the other possible string objects (std::string in this case)
 *
 * @param str Source string
 * @return Copy of the source string
 */
inline const std::string AsString(const std::string& str)
{
    return str;
}
/*!
 * \brief Gets std::string from std::wstring
 *
 * Helper method for use in template context which gets std::string from the other possible string objects (std::wstring in this case)
 * Conversion wchar_t -> char is performing
 *
 * @param str Source string
 * @return Converted source string
 */
inline std::string AsString(const std::wstring& str)
{
    return ConvertString<std::string>(str);
}
/*!
 * \brief Gets std::string from std::string_view
 *
 * Helper method for use in template context which gets std::string from the other possible string objects (std::string_view in this case)
 *
 * @param str Source string
 * @return Copy of the source string
 */
inline std::string AsString(const std::string_view& str)
{
    return std::string(str.begin(), str.end());
}
/*!
 * \brief Gets std::string from std::wstring_view
 *
 * Helper method for use in template context which gets std::string from the other possible string objects (std::wstring_view in this case)
 * Conversion wchar_t -> char is performing
 *
 * @param str Source string
 * @return Converted source string
 */
inline std::string AsString(const std::wstring_view& str)
{
    return ConvertString<std::string>(str);
}
/*!
 * \brief Gets std::wstring from std::wstring
 *
 * Helper method for use in template context which gets std::wstring from the other possible string objects (std::wstring in this case)
 *
 * @param str Source string
 * @return Copy of the source string
 */
inline const std::wstring AsWString(const std::wstring& str)
{
    return str;
}
/*!
 * \brief Gets std::wstring from std::string
 *
 * Helper method for use in template context which gets std::wstring from the other possible string objects (std::string in this case)
 * Conversion char -> wchar_t is performing
 *
 * @param str Source string
 * @return Converted source string
 */
inline std::wstring AsWString(const std::string& str)
{
    return ConvertString<std::wstring>(str);
}
/*!
 * \brief Gets std::wstring from std::wstring_view
 *
 * Helper method for use in template context which gets std::wstring from the other possible string objects (std::wstring_view in this case)
 *
 * @param str Source string
 * @return Copy of the source string
 */
inline std::wstring AsWString(const std::wstring_view& str)
{
    return std::wstring(str.begin(), str.end());
}
/*!
 * \brief Gets std::wstring from std::string_view
 *
 * Helper method for use in template context which gets std::wstring from the other possible string objects (std::string_view in this case)
 * Conversion char -> wchar_t is performing
 *
 * @param str Source string
 * @return Converted source string
 */
inline std::wstring AsWString(const std::string_view& str)
{
    return ConvertString<std::wstring>(str);
}

namespace detail
{
struct StringGetter
{
    template<typename CharT>
    std::string operator()(const std::basic_string<CharT>& str) const
    {
        return AsString(str);
    }
    template<typename CharT>
    std::string operator()(const std::basic_string_view<CharT>& str) const
    {
        return AsString(str);
    }

    template<typename T>
    std::string operator()(T&&) const
    {
        return std::string();
    }
};

struct WStringGetter
{
    template<typename CharT>
    std::wstring operator()(const std::basic_string<CharT>& str) const
    {
        return AsWString(str);
    }
    template<typename CharT>
    std::wstring operator()(const std::basic_string_view<CharT>& str) const
    {
        return AsWString(str);
    }

    template<typename T>
    std::wstring operator()(T&&) const
    {
        return std::wstring();
    }
};
} // namespace detail
/*!
 * \brief Gets std::string from the arbitrary \ref Value
 *
 * Helper method for use in template context which gets std::string from the other possible string objects (Value in this case).
 * Conversion wchar_t -> char is performing if needed. In case of non-string object actually stored in the \ref Value
 * empty string is returned.
 *
 * @param val Source string
 * @return Extracted or empty string
 */
inline std::string AsString(const Value& val)
{
    return std::visit(detail::StringGetter(), val.data());
}
/*!
 * \brief Gets std::wstring from the arbitrary \ref Value
 *
 * Helper method for use in template context which gets std::wstring from the other possible string objects (Value in this case).
 * Conversion char -> wchar_t is performing if needed. In case of non-string object actually stored in the \ref Value
 * empty string is returned.
 *
 * @param val Source string
 * @return Extracted or empty string
 */
inline std::wstring AsWString(const Value& val)
{
    return std::visit(detail::WStringGetter(), val.data());
}
} // namespace jinja2

#endif // JINJA2CPP_STRING_HELPERS_H
