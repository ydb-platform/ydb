#include "helpers.h"

#include <fmt/format.h>
#include <fmt/xchar.h>
#include <jinja2cpp/error_info.h>
#include <iterator>

namespace
{
template<typename FmtCtx>
struct ValueRenderer
{
    using CharT = typename FmtCtx::char_type;
    FmtCtx* ctx;

    explicit ValueRenderer(FmtCtx* c)
        : ctx(c)
    {
    }

    constexpr void operator()(bool val) const {
        fmt::format_to(
                ctx->out(),
                UNIVERSAL_STR("{}").GetValue<CharT>(),
                (val ? UNIVERSAL_STR("True").GetValue<CharT>(): UNIVERSAL_STR("False").GetValue<CharT>()));
    }
    void operator()(const jinja2::EmptyValue&) const { fmt::format_to(ctx->out(), UNIVERSAL_STR("").GetValue<CharT>()); }
    template<typename CharU>
    void operator()(const std::basic_string<CharU>& val) const
    {
        fmt::format_to(ctx->out(), UNIVERSAL_STR("{}").GetValue<CharT>(), jinja2::ConvertString<std::basic_string<CharT>>(val));
    }

    template<typename CharU>
    void operator()(const std::basic_string_view<CharU>& val) const
    {
        fmt::format_to(ctx->out(), UNIVERSAL_STR("{}").GetValue<CharT>(), jinja2::ConvertString<std::basic_string<CharT>>(val));
    }

    void operator()(const jinja2::ValuesList& vals) const
    {
        fmt::format_to(ctx->out(), UNIVERSAL_STR("{{").GetValue<CharT>());
        bool isFirst = true;
        for (auto& val : vals)
        {
            if (isFirst)
                isFirst = false;
            else
                fmt::format_to(ctx->out(), UNIVERSAL_STR(", ").GetValue<CharT>());
            std::visit(ValueRenderer<FmtCtx>(ctx), val.data());
        }
        fmt::format_to(ctx->out(), UNIVERSAL_STR("}}").GetValue<CharT>());
    }

    void operator()(const jinja2::ValuesMap& vals) const
    {
        fmt::format_to(ctx->out(), UNIVERSAL_STR("{{").GetValue<CharT>());
        bool isFirst = true;
        for (auto& val : vals)
        {
            if (isFirst)
                isFirst = false;
            else
                fmt::format_to(ctx->out(), UNIVERSAL_STR(", ").GetValue<CharT>());

            fmt::format_to(ctx->out(), UNIVERSAL_STR("{{\"{}\",").GetValue<CharT>(), jinja2::ConvertString<std::basic_string<CharT>>(val.first));
            std::visit(ValueRenderer<FmtCtx>(ctx), val.second.data());
            fmt::format_to(ctx->out(), UNIVERSAL_STR("}}").GetValue<CharT>());
        }
        fmt::format_to(ctx->out(), UNIVERSAL_STR("}}").GetValue<CharT>());
    }

    template<typename T>
    void operator()(const jinja2::RecWrapper<T>& val) const
    {
        this->operator()(const_cast<const T&>(*val));
    }

    void operator()(const jinja2::GenericMap& /*val*/) const {}

    void operator()(const jinja2::GenericList& /*val*/) const {}

    void operator()(const jinja2::UserCallable& /*val*/) const {}

    template<typename T>
    void operator()(const T& val) const
    {
        fmt::format_to(ctx->out(), UNIVERSAL_STR("{}").GetValue<CharT>(), val);
    }
};
} // namespace

namespace fmt
{
template<typename CharT>
struct formatter<jinja2::Value, CharT>
{
    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx)
    {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const jinja2::Value& val, FormatContext& ctx)
    {
        std::visit(ValueRenderer<FormatContext>(&ctx), val.data());
        return fmt::format_to(ctx.out(), UNIVERSAL_STR("").GetValue<CharT>());
    }
};
} // namespace fmt

namespace jinja2
{

template<typename CharT>
void RenderErrorInfo(std::basic_string<CharT>& result, const ErrorInfoTpl<CharT>& errInfo)
{
    using string_t = std::basic_string<CharT>;
    auto out = fmt::basic_memory_buffer<CharT>();

    auto& loc = errInfo.GetErrorLocation();

    fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("{}:{}:{}: error: ").GetValue<CharT>(), ConvertString<string_t>(loc.fileName), loc.line, loc.col);
    ErrorCode errCode = errInfo.GetCode();
    switch (errCode)
    {
    case ErrorCode::Unspecified:
            fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("Parse error").GetValue<CharT>());
            break;
    case ErrorCode::UnexpectedException:
    {
        auto& extraParams = errInfo.GetExtraParams();
        fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("Unexpected exception occurred during template processing. Exception: {}").GetValue<CharT>(), extraParams[0]);
        break;
    }
    case ErrorCode::MetadataParseError:
    {
        auto& extraParams = errInfo.GetExtraParams();
        fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("Error occurred during template metadata parsing. Error: {}").GetValue<CharT>(), extraParams[0]);
        break;
    }
    case ErrorCode::YetUnsupported:
        fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("This feature has not been supported yet").GetValue<CharT>());
        break;
    case ErrorCode::FileNotFound:
        fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("File not found").GetValue<CharT>());
        break;
    case ErrorCode::ExpectedStringLiteral:
        fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("String expected").GetValue<CharT>());
        break;
    case ErrorCode::ExpectedIdentifier:
        fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("Identifier expected").GetValue<CharT>());
        break;
    case ErrorCode::ExpectedSquareBracket:
        fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("']' expected").GetValue<CharT>());
        break;
    case ErrorCode::ExpectedRoundBracket:
        fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("')' expected").GetValue<CharT>());
        break;
    case ErrorCode::ExpectedCurlyBracket:
        fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("'}}' expected").GetValue<CharT>());
        break;
    case ErrorCode::ExpectedToken:
    {
        auto& extraParams = errInfo.GetExtraParams();
        fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("Unexpected token '{}'").GetValue<CharT>(), extraParams[0]);
        if (extraParams.size() > 1)
        {
            fmt::format_to(std::back_inserter(out), UNIVERSAL_STR(". Expected: ").GetValue<CharT>());
            for (std::size_t i = 1; i < extraParams.size(); ++ i)
            {
                if (i != 1)
                    fmt::format_to(std::back_inserter(out), UNIVERSAL_STR(", ").GetValue<CharT>());
                fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("\'{}\'").GetValue<CharT>(), extraParams[i]);
            }
        }
        break;
    }
    case ErrorCode::ExpectedExpression:
    {
        auto& extraParams = errInfo.GetExtraParams();
        fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("Expected expression, got: '{}'").GetValue<CharT>(), extraParams[0]);
        break;
    }
    case ErrorCode::ExpectedEndOfStatement:
    {
        auto& extraParams = errInfo.GetExtraParams();
        fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("Expected end of statement, got: '{}'").GetValue<CharT>(), extraParams[0]);
        break;
    }
    case ErrorCode::ExpectedRawEnd:
        fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("Expected end of raw block").GetValue<CharT>());
        break;
    case ErrorCode::ExpectedMetaEnd:
        fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("Expected end of meta block").GetValue<CharT>());
        break;
    case ErrorCode::UnexpectedToken:
    {
        auto& extraParams = errInfo.GetExtraParams();
        fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("Unexpected token: '{}'").GetValue<CharT>(), extraParams[0]);
        break;
    }
    case ErrorCode::UnexpectedStatement:
    {
        auto& extraParams = errInfo.GetExtraParams();
        fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("Unexpected statement: '{}'").GetValue<CharT>(), extraParams[0]);
        break;
    }
    case ErrorCode::UnexpectedCommentBegin:
        fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("Unexpected comment begin").GetValue<CharT>());
        break;
    case ErrorCode::UnexpectedCommentEnd:
        fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("Unexpected comment end").GetValue<CharT>());
        break;
    case ErrorCode::UnexpectedRawBegin:
        fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("Unexpected raw block begin").GetValue<CharT>());
        break;
    case ErrorCode::UnexpectedRawEnd:
        fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("Unexpected raw block end").GetValue<CharT>());
        break;
    case ErrorCode::UnexpectedMetaBegin:
        fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("Unexpected meta block begin").GetValue<CharT>());
        break;
    case ErrorCode::UnexpectedMetaEnd:
        fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("Unexpected meta block end").GetValue<CharT>());
        break;
    case ErrorCode::UnexpectedExprBegin:
        fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("Unexpected expression block begin").GetValue<CharT>());
        break;
    case ErrorCode::UnexpectedExprEnd:
        fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("Unexpected expression block end").GetValue<CharT>());
        break;
    case ErrorCode::UnexpectedStmtBegin:
        fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("Unexpected statement block begin").GetValue<CharT>());
        break;
    case ErrorCode::UnexpectedStmtEnd:
        fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("Unexpected statement block end").GetValue<CharT>());
        break;
    case ErrorCode::TemplateNotParsed:
        fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("Template not parsed").GetValue<CharT>());
        break;
    case ErrorCode::TemplateNotFound:
        fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("Template(s) not found: {}").GetValue<CharT>(), errInfo.GetExtraParams()[0]);
        break;
    case ErrorCode::InvalidTemplateName:
        fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("Invalid template name: {}").GetValue<CharT>(), errInfo.GetExtraParams()[0]);
        break;
    case ErrorCode::InvalidValueType:
        fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("Invalid value type").GetValue<CharT>());
        break;
    case ErrorCode::ExtensionDisabled:
        fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("Extension disabled").GetValue<CharT>());
        break;
    case ErrorCode::TemplateEnvAbsent:
        fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("Template environment doesn't set").GetValue<CharT>());
        break;
    }
    fmt::format_to(std::back_inserter(out), UNIVERSAL_STR("\n{}").GetValue<CharT>(), errInfo.GetLocationDescr());
    result = to_string(out);
}

template<>
std::string ErrorInfoTpl<char>::ToString() const
{
    std::string result;
    RenderErrorInfo(result, *this);
    return result;
}

template<>
std::wstring ErrorInfoTpl<wchar_t>::ToString() const
{
    std::wstring result;
    RenderErrorInfo(result, *this);
    return result;
}

std::ostream& operator << (std::ostream& os, const ErrorInfo& res)
{
    os << res.ToString();
    return os;
}
std::wostream& operator << (std::wostream& os, const ErrorInfoW& res)
{
    os << res.ToString();
    return os;
}
} // namespace jinja2
