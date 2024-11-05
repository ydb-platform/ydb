#ifndef JINJA2CPP_ERROR_INFO_H
#define JINJA2CPP_ERROR_INFO_H

#include "config.h"
#include "value.h"

#include <iostream>
#include <type_traits>
#include <vector>

namespace jinja2
{
/*!
 * \brief Type of the error
 */
enum class ErrorCode
{
    Unspecified = 0,              //!< Error is unspecified
    UnexpectedException = 1,      //!< Generic exception occurred during template parsing or execution. ExtraParams[0] contains `what()` string of the exception
    YetUnsupported,               //!< Feature of the jinja2 specification which yet not supported
    FileNotFound,                 //!< Requested file was not found. ExtraParams[0] contains name of the file
    ExtensionDisabled,            //!< Particular jinja2 extension disabled in the settings
    TemplateEnvAbsent,            //!< Template uses `extend`, `import`, `from` or `include` features but it's loaded without the template environment set
    TemplateNotFound,             //!< Template with the specified name was not found. ExtraParams[0] contains name of the file
    TemplateNotParsed,            //!< Template was not parsed
    InvalidValueType,             //!< Invalid type of the value in the particular context
    InvalidTemplateName,          //!< Invalid name of the template. ExtraParams[0] contains the name
    MetadataParseError,           //!< Invalid name of the template. ExtraParams[0] contains the name
    ExpectedStringLiteral = 1001, //!< String literal expected
    ExpectedIdentifier,           //!< Identifier expected
    ExpectedSquareBracket,        //!< ']' expected
    ExpectedRoundBracket,         //!< ')' expected
    ExpectedCurlyBracket,         //!< '}' expected
    ExpectedToken,                //!< Specific token(s) expected. ExtraParams[0] contains the actual token, rest of ExtraParams contain set of expected tokens
    ExpectedExpression,           //!< Expression expected
    ExpectedEndOfStatement,       //!< End of statement expected. ExtraParams[0] contains the expected end of statement tag
    ExpectedRawEnd,               //!< {% endraw %} expected
    ExpectedMetaEnd,              //!< {% endmeta %} expected
    UnexpectedToken,              //!< Unexpected token. ExtraParams[0] contains the invalid token
    UnexpectedStatement,          //!< Unexpected statement. ExtraParams[0] contains the invalid statement tag
    UnexpectedCommentBegin,       //!< Unexpected comment block begin (`{#`)
    UnexpectedCommentEnd,         //!< Unexpected comment block end (`#}`)
    UnexpectedExprBegin,          //!< Unexpected expression block begin (`{{`)
    UnexpectedExprEnd,            //!< Unexpected expression block end (`}}`)
    UnexpectedStmtBegin,          //!< Unexpected statement block begin (`{%`)
    UnexpectedStmtEnd,            //!< Unexpected statement block end (`%}`)
    UnexpectedRawBegin,           //!< Unexpected raw block begin {% raw %}
    UnexpectedRawEnd,             //!< Unexpected raw block end {% endraw %}
    UnexpectedMetaBegin,          //!< Unexpected meta block begin {% meta %}
    UnexpectedMetaEnd,            //!< Unexpected meta block end {% endmeta %}
};

/*!
 * \brief Information about the source location of the error
 */
struct SourceLocation
{
    //! Name of the file
    std::string fileName;
    //! Line number (1-based)
    unsigned line = 0;
    //! Column number (1-based)
    unsigned col = 0;
};

template<typename CharT>
/*!
 * \brief Detailed information about the parse-time or render-time error
 *
 * If template parsing or rendering fails the detailed error information is provided. Exact specialization of ErrorInfoTpl is an object which contains
 * this information. Type of specialization depends on type of the template object: \ref ErrorInfo for \ref Template and \ref ErrorInfoW for \ref TemplateW.
 *
 * Detailed information about an error contains:
 * - Error code
 * - Error location
 * - Other locations related to the error
 * - Description of the location
 * - Extra parameters of the error
 *
 * @tparam CharT Character type which was used in template parser
 */
class ErrorInfoTpl
{
public:
    struct Data
    {
        ErrorCode code = ErrorCode::Unspecified;
        SourceLocation srcLoc;
        std::vector<SourceLocation> relatedLocs;
        std::vector<Value> extraParams;
        std::basic_string<CharT> locationDescr;
    };

    //! Default constructor
    ErrorInfoTpl() = default;
    //! Initializing constructor from error description
    explicit ErrorInfoTpl(Data data)
        : m_errorData(std::move(data))
    {}

    //! Copy constructor
    ErrorInfoTpl(const ErrorInfoTpl<CharT>&) = default;
    //! Move constructor
    ErrorInfoTpl(ErrorInfoTpl<CharT>&& val) noexcept
        : m_errorData(std::move(val.m_errorData))
    { }

    //! Destructor
    ~ErrorInfoTpl() noexcept = default;

    //! Copy-assignment operator
    ErrorInfoTpl& operator =(const ErrorInfoTpl<CharT>&) = default;
    //! Move-assignment operator
    ErrorInfoTpl& operator =(ErrorInfoTpl<CharT>&& val) noexcept
    {
        if (this == &val)
            return *this;

        std::swap(m_errorData.code, val.m_errorData.code);
        std::swap(m_errorData.srcLoc, val.m_errorData.srcLoc);
        std::swap(m_errorData.relatedLocs, val.m_errorData.relatedLocs);
        std::swap(m_errorData.extraParams, val.m_errorData.extraParams);
        std::swap(m_errorData.locationDescr, val.m_errorData.locationDescr);

        return *this;
    }

    //! Return code of the error
    ErrorCode GetCode() const
    {
        return m_errorData.code;
    }

    //! Return error location in the template file
    auto& GetErrorLocation() const
    {
        return m_errorData.srcLoc;
    }

    //! Return locations, related to the main error location
    auto& GetRelatedLocations() const
    {
        return m_errorData.relatedLocs;
    }

    /*!
     * \brief Return location description
     *
     * Return string of two lines. First line is contents of the line with error. Second highlight the exact position within line. For instance:
     * ```
     * {% for i in range(10) endfor%}
     *                    ---^-------
     * ```
     *
     * @return Location description
     */
    const std::basic_string<CharT>& GetLocationDescr() const
    {
        return m_errorData.locationDescr;
    }

    /*!
     * \brief Return extra params of the error
     *
     * Extra params is a additional details assiciated with the error. For instance, name of the file which wasn't opened
     *
     * @return Vector with extra error params
     */
    auto& GetExtraParams() const { return m_errorData.extraParams; }

    //! Convert error to the detailed string representation
    JINJA2CPP_EXPORT std::basic_string<CharT> ToString() const;

private:
    Data m_errorData;
};

using ErrorInfo = ErrorInfoTpl<char>;
using ErrorInfoW = ErrorInfoTpl<wchar_t>;

JINJA2CPP_EXPORT std::ostream& operator<<(std::ostream& os, const ErrorInfo& res);
JINJA2CPP_EXPORT std::wostream& operator<<(std::wostream& os, const ErrorInfoW& res);
} // namespace jinja2

#endif // JINJA2CPP_ERROR_INFO_H
