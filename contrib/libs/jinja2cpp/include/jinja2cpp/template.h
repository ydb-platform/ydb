#ifndef JINJA2CPP_TEMPLATE_H
#define JINJA2CPP_TEMPLATE_H

#include "config.h"
#include "error_info.h"
#include "value.h"

#include <contrib/restricted/expected-lite/include/nonstd/expected.hpp>

#include <iostream>
#include <memory>
#include <string>

namespace jinja2
{
class JINJA2CPP_EXPORT ITemplateImpl;
class JINJA2CPP_EXPORT TemplateEnv;
template<typename CharT>
class TemplateImpl;
template<typename U>
using Result = nonstd::expected<U, ErrorInfo>;
template<typename U>
using ResultW = nonstd::expected<U, ErrorInfoW>;

template<typename CharT>
struct MetadataInfo
{
    std::string metadataType;
    std::basic_string_view<CharT> metadata;
    SourceLocation location;
};

/*!
 * \brief Template object which is used to render narrow char templates
 *
 * This class is a main class for rendering narrow char templates. It can be used independently or together with
 * \ref TemplateEnv. In the second case it's possible to use templates inheritance and extension.
 *
 * Basic usage of Template class:
 * ```c++
 * std::string source = "Hello World from Parser!";
 *
 * jinja2::Template tpl;
 * tpl.Load(source);
 * std::string result = tpl.RenderAsString(ValuesMap{}).value();
 * ```
 */
class JINJA2CPP_EXPORT Template
{
public:
    /*!
     * \brief Default constructor
     */
    Template()
        : Template(nullptr)
    {
    }
    /*!
     * \brief Initializing constructor
     *
     * Creates instance of the template with the specified template environment object
     *
     * @param env Template environment object which created template should refer to
     */
    explicit Template(TemplateEnv* env);
    /*!
     * Destructor
     */
    ~Template();

    /*!
     * \brief Load template from the zero-terminated narrow char string
     *
     * Takes specified narrow char string and parses it as a Jinja2 template. In case of error returns detailed
     * diagnostic
     *
     * @param tpl      Zero-terminated narrow char string with template description
     * @param tplName  Optional name of the template (for the error reporting purposes)
     *
     * @return Either noting or instance of \ref ErrorInfoTpl as an error
     */
    Result<void> Load(const char* tpl, std::string tplName = std::string());
    /*!
     * \brief Load template from the std::string
     *
     * Takes specified std::string object and parses it as a Jinja2 template. In case of error returns detailed
     * diagnostic
     *
     * @param str      std::string object with template description
     * @param tplName  Optional name of the template (for the error reporting purposes)
     *
     * @return Either noting or instance of \ref ErrorInfoTpl as an error
     */
    Result<void> Load(const std::string& str, std::string tplName = std::string());
    /*!
     * \brief Load template from the stream
     *
     * Takes specified stream object and parses it as a source of Jinja2 template. In case of error returns detailed
     * diagnostic
     *
     * @param stream   Stream object with template description
     * @param tplName  Optional name of the template (for the error reporting purposes)
     *
     * @return Either noting or instance of \ref ErrorInfoTpl as an error
     */
    Result<void> Load(std::istream& stream, std::string tplName = std::string());
    /*!
     * \brief Load template from the specified file
     *
     * Loads file with the specified name and parses it as a source of Jinja2 template. In case of error returns
     * detailed diagnostic
     *
     * @param fileName Name of the file to load
     *
     * @return Either noting or instance of \ref ErrorInfoTpl as an error
     */
    Result<void> LoadFromFile(const std::string& fileName);

    /*!
     * \brief Render previously loaded template to the narrow char stream
     *
     * Renders previously loaded template to the specified narrow char stream and specified set of params.
     *
     * @param os      Stream to render template to
     * @param params  Set of params which should be passed to the template engine and can be used within the template
     *
     * @return Either noting or instance of \ref ErrorInfoTpl as an error
     */
    Result<void> Render(std::ostream& os, const ValuesMap& params);
    /*!
     * \brief Render previously loaded template to the narrow char string
     *
     * Renders previously loaded template as a narrow char string and with specified set of params.
     *
     * @param params  Set of params which should be passed to the template engine and can be used within the template
     *
     * @return Either rendered string or instance of \ref ErrorInfoTpl as an error
     */
    Result<std::string> RenderAsString(const ValuesMap& params);
    /*!
     * \brief Get metadata, provided in the {% meta %} tag
     *
     * @return Parsed metadata as a generic map value or instance of \ref ErrorInfoTpl as an error
     */
    Result<GenericMap> GetMetadata();
    /*!
     * \brief Get non-parsed metadata, provided in the {% meta %} tag
     *
     * @return Non-parsed metadata information or instance of \ref ErrorInfoTpl as an error
     */
    Result<MetadataInfo<char>> GetMetadataRaw();

    /* !
     * \brief compares to an other object of the same type
     *
     * @return true if equal
     */
    bool IsEqual(const Template& other) const;

private:
    std::shared_ptr<ITemplateImpl> m_impl;
    friend class TemplateImpl<char>;
};

bool operator==(const Template& lhs, const Template& rhs);
bool operator!=(const Template& lhs, const Template& rhs);

/*!
 * \brief Template object which is used to render wide char templates
 *
 * This class is a main class for rendering wide char templates. It can be used independently or together with
 * \ref TemplateEnv. In the second case it's possible to use templates inheritance and extension.
 *
 * Basic usage of Template class:
 * ```c++
 * std::string source = "Hello World from Parser!";
 *
 * jinja2::Template tpl;
 * tpl.Load(source);
 * std::string result = tpl.RenderAsString(ValuesMap{}).value();
 * ```
*/
class JINJA2CPP_EXPORT TemplateW
{
public:
    /*!
     * \brief Default constructor
     */
    TemplateW()
        : TemplateW(nullptr)
    {
    }
    /*!
     * \brief Initializing constructor
     *
     * Creates instance of the template with the specified template environment object
     *
     * @param env Template environment object which created template should refer to
     */
    explicit TemplateW(TemplateEnv* env);
    /*!
     * Destructor
     */
    ~TemplateW();

    /*!
     * \brief Load template from the zero-terminated wide char string
     *
     * Takes specified wide char string and parses it as a Jinja2 template. In case of error returns detailed
     * diagnostic
     *
     * @param tpl      Zero-terminated wide char string with template description
     * @param tplName  Optional name of the template (for the error reporting purposes)
     *
     * @return Either noting or instance of \ref ErrorInfoTpl as an error
     */
    ResultW<void> Load(const wchar_t* tpl, std::string tplName = std::string());
    /*!
     * \brief Load template from the std::wstring
     *
     * Takes specified std::wstring object and parses it as a Jinja2 template. In case of error returns detailed
     * diagnostic
     *
     * @param str      std::wstring object with template description
     * @param tplName  Optional name of the template (for the error reporting purposes)
     *
     * @return Either noting or instance of \ref ErrorInfoTpl as an error
     */
    ResultW<void> Load(const std::wstring& str, std::string tplName = std::string());
    /*!
     * \brief Load template from the stream
     *
     * Takes specified stream object and parses it as a source of Jinja2 template. In case of error returns detailed
     * diagnostic
     *
     * @param stream   Stream object with template description
     * @param tplName  Optional name of the template (for the error reporting purposes)
     *
     * @return Either noting or instance of \ref ErrorInfoTpl as an error
     */
    ResultW<void> Load(std::wistream& stream, std::string tplName = std::string());
    /*!
     * \brief Load template from the specified file
     *
     * Loads file with the specified name and parses it as a source of Jinja2 template. In case of error returns
     * detailed diagnostic
     *
     * @param fileName Name of the file to load
     *
     * @return Either noting or instance of \ref ErrorInfoTpl as an error
     */
    ResultW<void> LoadFromFile(const std::string& fileName);

    /*!
     * \brief Render previously loaded template to the wide char stream
     *
     * Renders previously loaded template to the specified wide char stream and specified set of params.
     *
     * @param os      Stream to render template to
     * @param params  Set of params which should be passed to the template engine and can be used within the template
     *
     * @return Either noting or instance of \ref ErrorInfoTpl as an error
     */
    ResultW<void> Render(std::wostream& os, const ValuesMap& params);
    /*!
     * \brief Render previously loaded template to the wide char string
     *
     * Renders previously loaded template as a wide char string and with specified set of params.
     *
     * @param params  Set of params which should be passed to the template engine and can be used within the template
     *
     * @return Either rendered string or instance of \ref ErrorInfoTpl as an error
     */
    ResultW<std::wstring> RenderAsString(const ValuesMap& params);
    /*!
     * \brief Get metadata, provided in the {% meta %} tag
     *
     * @return Parsed metadata as a generic map value or instance of \ref ErrorInfoTpl as an error
     */
    ResultW<GenericMap> GetMetadata();
    /*!
     * \brief Get non-parsed metadata, provided in the {% meta %} tag
     *
     * @return Non-parsed metadata information or instance of \ref ErrorInfoTpl as an error
     */
    ResultW<MetadataInfo<wchar_t>> GetMetadataRaw();

    /* !
     * \brief compares to an other object of the same type
     *
     * @return true if equal
     */
    bool IsEqual(const TemplateW& other) const;

private:
    std::shared_ptr<ITemplateImpl> m_impl;
    friend class TemplateImpl<wchar_t>;
};

bool operator==(const TemplateW& lhs, const TemplateW& rhs);
bool operator!=(const TemplateW& lhs, const TemplateW& rhs);

} // namespace jinja2

#endif // JINJA2CPP_TEMPLATE_H
