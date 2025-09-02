#include "filters.h"
#include "testers.h"
#include "value_visitors.h"
#include "value_helpers.h"

#include <algorithm>
#include <numeric>
#include <regex>
#include <sstream>

#include <boost/algorithm/string/trim_all.hpp>
#include <boost/algorithm/string/replace.hpp>

namespace ba = boost::algorithm;

namespace jinja2
{

namespace filters
{

template<typename D>
struct StringEncoder : public visitors::BaseVisitor<TargetString>
{
    using BaseVisitor::operator();

    template<typename CharT>
    TargetString operator() (const std::basic_string<CharT>& str) const
    {
        std::basic_string<CharT> result;

        for (auto& ch : str)
        {
            static_cast<const D*>(this)->EncodeChar(ch, [&result](auto ... chs) {AppendChar(result, chs...);});
        }

        return TargetString(std::move(result));
    }

    template<typename CharT>
    TargetString operator() (const std::basic_string_view<CharT>& str) const
    {
        std::basic_string<CharT> result;

        for (auto& ch : str)
        {
            static_cast<const D*>(this)->EncodeChar(ch, [&result](auto ... chs) {AppendChar(result, chs...);});
        }

        return TargetString(std::move(result));
    }

    template<typename Str, typename CharT>
    static void AppendChar(Str& str, CharT ch)
    {
        str.push_back(static_cast<typename Str::value_type>(ch));
    }
    template<typename Str, typename CharT, typename ... Args>
    static void AppendChar(Str& str, CharT ch, Args ... chs)
    {
        str.push_back(static_cast<typename Str::value_type>(ch));
        AppendChar(str, chs...);
    }
};

template<typename Fn>
struct GenericStringEncoder : public StringEncoder<GenericStringEncoder<Fn>>
{
    GenericStringEncoder(Fn fn) : m_fn(std::move(fn)) {}

    template<typename CharT, typename AppendFn>
    void EncodeChar(CharT ch, AppendFn&& fn) const
    {
        m_fn(ch, std::forward<AppendFn>(fn));
    }

    mutable Fn m_fn;
};

struct UrlStringEncoder : public StringEncoder<UrlStringEncoder>
{
    template<typename CharT, typename Fn>
    void EncodeChar(CharT ch, Fn&& fn) const
    {
        enum EncodeStyle
        {
            None,
            Percent
        };

        EncodeStyle encStyle = None;
        switch (ch)
        {
        case ' ':
            fn('+');
            return;
        case '+': case '\"': case '%': case '-':
        case '!': case '#':  case '$': case '&':
        case '\'': case '(': case ')': case '*':
        case ',': case '/':  case ':': case ';':
        case '=': case '?':  case '@': case '[':
        case ']':
            encStyle = Percent;
            break;
        default:
            if (AsUnsigned(ch) > 0x7f)
                encStyle = Percent;
            break;
        }

        if (encStyle == None)
        {
            fn(ch);
            return;
        }
        union
        {
            uint32_t intCh;
            uint8_t chars[4];
        };
        intCh = AsUnsigned(ch);
        if (intCh > 0xffffff)
            DoPercentEncoding(chars[3], fn);
        if (intCh > 0xffff)
            DoPercentEncoding(chars[2], fn);
        if (intCh > 0xff)
            DoPercentEncoding(chars[1], fn);
        DoPercentEncoding(chars[0], fn);
    }

    template<typename Fn>
    void DoPercentEncoding(uint8_t ch, Fn&& fn) const
    {
        char chars[] = "0123456789ABCDEF";
        int ch1 = static_cast<int>(chars[(ch & 0xf0) >> 4]);
        int ch2 = static_cast<int>(chars[ch & 0x0f]);
        fn('%', ch1, ch2);
    }

    template<typename Ch, size_t SZ>
    struct ToUnsigned;

    template<typename Ch>
    struct ToUnsigned<Ch, 1>
    {
        static auto Cast(Ch ch) {return static_cast<uint8_t>(ch);}
    };

    template<typename Ch>
    struct ToUnsigned<Ch, 2>
    {
        static auto Cast(Ch ch) {return static_cast<uint16_t>(ch);}
    };

    template<typename Ch>
    struct ToUnsigned<Ch, 4>
    {
        static auto Cast(Ch ch) {return static_cast<uint32_t>(ch);}
    };

    template<typename Ch>
    auto AsUnsigned(Ch ch) const
    {
        return static_cast<uint32_t>(ToUnsigned<Ch, sizeof(Ch)>::Cast(ch));
    }
};

StringConverter::StringConverter(FilterParams params, StringConverter::Mode mode)
    : m_mode(mode)
{
    switch (m_mode)
    {
    case ReplaceMode:
        ParseParams({{"old", true}, {"new", true}, {"count", false, static_cast<int64_t>(0)}}, params);
        break;
    case TruncateMode:
        ParseParams({{"length", false, static_cast<int64_t>(255)}, {"killwords", false, false}, {"end", false, std::string("...")}, {"leeway", false}}, params);
        break;
    case CenterMode:
        ParseParams({{"width", false, static_cast<int64_t>(80)}}, params);
        break;
    default: break;
    }
}

InternalValue StringConverter::Filter(const InternalValue& baseVal, RenderContext& context)
{
    TargetString result;

    auto isAlpha = ba::is_alpha();
    auto isAlNum = ba::is_alnum();

    switch (m_mode)
    {
    case TrimMode:
        result = ApplyStringConverter(baseVal, [](auto strView) -> TargetString {
            auto str = sv_to_string(strView);
            ba::trim_all(str);
            return TargetString(str);
        });
        break;
    case TitleMode:
        result = ApplyStringConverter<GenericStringEncoder>(baseVal, [isDelim = true, &isAlpha, &isAlNum](auto ch, auto&& fn) mutable {
            if (isDelim && isAlpha(ch))
            {
                isDelim = false;
                fn(std::toupper(ch, std::locale()));
                return;
            }

            isDelim = !isAlNum(ch);
            fn(ch);
        });
        break;
    case WordCountMode:
    {
        int64_t wc = 0;
        ApplyStringConverter<GenericStringEncoder>(baseVal, [isDelim = true, &wc, &isAlNum](auto ch, auto&&) mutable {
            if (isDelim && isAlNum(ch))
            {
                isDelim = false;
                wc ++;
                return;
            }
            isDelim = !isAlNum(ch);
        });
        return InternalValue(wc);
    }
    case UpperMode:
        result = ApplyStringConverter<GenericStringEncoder>(baseVal, [&isAlpha](auto ch, auto&& fn) mutable {
            if (isAlpha(ch))
                fn(std::toupper(ch, std::locale()));
            else
                fn(ch);
        });
        break;
    case LowerMode:
        result = ApplyStringConverter<GenericStringEncoder>(baseVal, [&isAlpha](auto ch, auto&& fn) mutable {
            if (isAlpha(ch))
                fn(std::tolower(ch, std::locale()));
            else
                fn(ch);
        });
        break;
    case ReplaceMode:
        result = ApplyStringConverter(baseVal, [this, &context](auto srcStr) -> TargetString {
            std::decay_t<decltype(srcStr)> emptyStrView;
            using CharT = typename decltype(emptyStrView)::value_type;
            std::basic_string<CharT> emptyStr;
            auto oldStr = GetAsSameString(srcStr, this->GetArgumentValue("old", context)).value_or(emptyStr);
            auto newStr = GetAsSameString(srcStr, this->GetArgumentValue("new", context)).value_or(emptyStr);
            auto count = ConvertToInt(this->GetArgumentValue("count", context));
            auto str = sv_to_string(srcStr);
            if (count == 0)
                ba::replace_all(str, oldStr, newStr);
            else
            {
                for (int64_t n = 0; n < count; ++ n)
                    ba::replace_first(str, oldStr, newStr);
            }
            return str;
        });
        break;
    case TruncateMode:
        result = ApplyStringConverter(baseVal, [this, &context, &isAlNum](auto srcStr) -> TargetString {
            std::decay_t<decltype(srcStr)> emptyStrView;
            using CharT = typename decltype(emptyStrView)::value_type;
            std::basic_string<CharT> emptyStr;
            auto length = ConvertToInt(this->GetArgumentValue("length", context));
            auto killWords = ConvertToBool(this->GetArgumentValue("killwords", context));
            auto end = GetAsSameString(srcStr, this->GetArgumentValue("end", context));
            auto leeway = ConvertToInt(this->GetArgumentValue("leeway", context), 5);
            if (static_cast<long long int>(srcStr.size()) <= length)
                return sv_to_string(srcStr);

            auto str = sv_to_string(srcStr);

            if (killWords)
            {
                if (static_cast<long long int>(str.size()) > (length + leeway))
                {
                    str.erase(str.begin() + static_cast<std::ptrdiff_t>(length), str.end());
                    str += end.value_or(emptyStr);
                }
                return str;
            }

            auto p = str.begin() + static_cast<std::ptrdiff_t>(length);
            if (leeway != 0)
            {
                for (; leeway != 0 && p != str.end() && isAlNum(*p); -- leeway, ++ p);
                if (p == str.end())
                    return TargetString(str);
            }

            if (isAlNum(*p))
            {
                for (; p != str.begin() && isAlNum(*p); -- p);
            }
            str.erase(p, str.end());
            ba::trim_right(str);
            str += end.value_or(emptyStr);

            return TargetString(std::move(str));
        });
        break;
    case UrlEncodeMode:
        result = Apply<UrlStringEncoder>(baseVal);
        break;
    case CapitalMode:
        result = ApplyStringConverter<GenericStringEncoder>(baseVal, [isFirstChar = true, &isAlpha](auto ch, auto&& fn) mutable {
            if (isAlpha(ch))
            {
                if (isFirstChar)
                    fn(std::toupper(ch, std::locale()));
                else
                    fn(std::tolower(ch, std::locale()));
            }
            else
                fn(ch);

            isFirstChar = false;
        });
        break;
    case EscapeHtmlMode:
        result = ApplyStringConverter<GenericStringEncoder>(baseVal, [](auto ch, auto&& fn) mutable {
            switch(ch)
            {
                case '<':
                    fn('&', 'l', 't', ';');
                    break;
                case '>':
                    fn('&', 'g', 't', ';');
                    break;
                case '&':
                    fn('&', 'a', 'm', 'p', ';');
                    break;
                case '\'':
                    fn('&', '#', '3', '9', ';');
                    break;
                case '\"':
                    fn('&', '#', '3', '4', ';');
                    break;
                default:
                    fn(ch);
                    break;
            }
        });
        break;
     case StriptagsMode:
        result = ApplyStringConverter(baseVal, [](auto srcStr) -> TargetString {
            auto str = sv_to_string(srcStr);
            using StringT = decltype(str);
            using CharT = typename StringT::value_type;
            static const std::basic_regex<CharT> STRIPTAGS_RE(UNIVERSAL_STR("(<!--.*?-->|<[^>]*>)").GetValueStr<CharT>());
            str = std::regex_replace(str, STRIPTAGS_RE, UNIVERSAL_STR("").GetValueStr<CharT>());
            ba::trim_all(str);
            static const StringT html_entities [] {
                UNIVERSAL_STR("&amp;").GetValueStr<CharT>(), UNIVERSAL_STR("&").GetValueStr<CharT>(),
                UNIVERSAL_STR("&apos;").GetValueStr<CharT>(), UNIVERSAL_STR("\'").GetValueStr<CharT>(),
                UNIVERSAL_STR("&gt;").GetValueStr<CharT>(), UNIVERSAL_STR(">").GetValueStr<CharT>(),
                UNIVERSAL_STR("&lt;").GetValueStr<CharT>(), UNIVERSAL_STR("<").GetValueStr<CharT>(),
                UNIVERSAL_STR("&quot;").GetValueStr<CharT>(), UNIVERSAL_STR("\"").GetValueStr<CharT>(),
                UNIVERSAL_STR("&#39;").GetValueStr<CharT>(), UNIVERSAL_STR("\'").GetValueStr<CharT>(),
                UNIVERSAL_STR("&#34;").GetValueStr<CharT>(), UNIVERSAL_STR("\"").GetValueStr<CharT>(),
            };
            for (auto it = std::begin(html_entities), end = std::end(html_entities); it < end; it += 2)
            {
                ba::replace_all(str, *it, *(it + 1));
            }
            return str;
        });
        break;
    case CenterMode:
        result = ApplyStringConverter(baseVal, [this, &context](auto srcStr) -> TargetString {
            auto width = ConvertToInt(this->GetArgumentValue("width", context));
            auto str = sv_to_string(srcStr);
            auto string_length = static_cast<long long int>(str.size());
            if (string_length >= width)
                return str;
            auto whitespaces = width - string_length;
            str = decltype(str)(static_cast<std::string::size_type>(whitespaces + 1) / 2, ' ') + str;
            str.append(static_cast<std::string::size_type>(whitespaces / 2), ' ');
            return TargetString(std::move(str));
        });
        break;
    default:
        break;
    }

    return std::move(result);
}

} // namespace filters
} // namespace jinja2
