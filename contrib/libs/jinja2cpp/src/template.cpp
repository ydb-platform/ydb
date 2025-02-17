#include "jinja2cpp/template.h"
#include "template_impl.h"

#include <fmt/format.h>

#include <fstream>
#include <sstream>

namespace jinja2
{
bool operator==(const Template& lhs, const Template& rhs)
{
    return lhs.IsEqual(rhs);
}

bool operator==(const TemplateW& lhs, const TemplateW& rhs)
{
    return lhs.IsEqual(rhs);
}

template<typename CharT>
auto GetImpl(std::shared_ptr<ITemplateImpl> impl)
{
    return static_cast<TemplateImpl<CharT>*>(impl.get());
}

Template::Template(TemplateEnv* env)
    : m_impl(new TemplateImpl<char>(env))
{

}

Template::~Template() = default;

Result<void> Template::Load(const char* tpl, std::string tplName)
{
    std::string t(tpl);
    auto result = GetImpl<char>(m_impl)->Load(std::move(t), std::move(tplName));
    return !result ? Result<void>() : nonstd::make_unexpected(std::move(result.get()));
}

Result<void> Template::Load(const std::string& str, std::string tplName)
{
    auto result = GetImpl<char>(m_impl)->Load(str, std::move(tplName));
    return !result ? Result<void>() : nonstd::make_unexpected(std::move(result.get()));
}

Result<void> Template::Load(std::istream& stream, std::string tplName)
{
    std::string t;

    while (stream.good() && !stream.eof())
    {
        char buff[0x10000];
        stream.read(buff, sizeof(buff));
        auto read = stream.gcount();
        if (read)
            t.append(buff, buff + read);
    }

    auto result = GetImpl<char>(m_impl)->Load(std::move(t), std::move(tplName));
    return !result ? Result<void>() : nonstd::make_unexpected(std::move(result.get()));
}

Result<void> Template::LoadFromFile(const std::string& fileName)
{
    std::ifstream file(fileName);

    if (!file.good())
        return Result<void>();

    return Load(file, fileName);
}

Result<void> Template::Render(std::ostream& os, const jinja2::ValuesMap& params)
{
    std::string buffer;
    auto result = GetImpl<char>(m_impl)->Render(buffer, params);

    if (!result)
        os.write(buffer.data(), buffer.size());

    return !result ? Result<void>() : nonstd::make_unexpected(std::move(result.get()));
}

Result<std::string> Template::RenderAsString(const jinja2::ValuesMap& params)
{
    std::string buffer;
    auto result = GetImpl<char>(m_impl)->Render(buffer, params);
    return !result ? Result<std::string>(std::move(buffer)) : Result<std::string>(nonstd::make_unexpected(std::move(result.get())));;
}

Result<GenericMap> Template::GetMetadata()
{
    return GetImpl<char>(m_impl)->GetMetadata();
}

Result<MetadataInfo<char>> Template::GetMetadataRaw()
{
    return GetImpl<char>(m_impl)->GetMetadataRaw();
}

bool Template::IsEqual(const Template& other) const
{
    return m_impl == other.m_impl;
}

TemplateW::TemplateW(TemplateEnv* env)
    : m_impl(new TemplateImpl<wchar_t>(env))
{

}

TemplateW::~TemplateW() = default;

ResultW<void> TemplateW::Load(const wchar_t* tpl, std::string tplName)
{
    std::wstring t(tpl);
    auto result = GetImpl<wchar_t>(m_impl)->Load(t, std::move(tplName));
    return !result ? ResultW<void>() : nonstd::make_unexpected(std::move(result.get()));
}

ResultW<void> TemplateW::Load(const std::wstring& str, std::string tplName)
{
    auto result = GetImpl<wchar_t>(m_impl)->Load(str, std::move(tplName));
    return !result ? ResultW<void>() : nonstd::make_unexpected(std::move(result.get()));
}

ResultW<void> TemplateW::Load(std::wistream& stream, std::string tplName)
{
    std::wstring t;

    while (stream.good() && !stream.eof())
    {
        wchar_t buff[0x10000];
        stream.read(buff, sizeof(buff));
        auto read = stream.gcount();
        if (read)
            t.append(buff, buff + read);
    }

    auto result = GetImpl<wchar_t>(m_impl)->Load(t, std::move(tplName));
    return !result ? ResultW<void>() : nonstd::make_unexpected(std::move(result.get()));
}

ResultW<void> TemplateW::LoadFromFile(const std::string& fileName)
{
    std::wifstream file(fileName);

    if (!file.good())
        return ResultW<void>();

    return Load(file, fileName);
}

ResultW<void> TemplateW::Render(std::wostream& os, const jinja2::ValuesMap& params)
{
    std::wstring buffer;
    auto result = GetImpl<wchar_t>(m_impl)->Render(buffer, params);
    if (!result)
        os.write(buffer.data(), buffer.size());
    return !result ? ResultW<void>() : ResultW<void>(nonstd::make_unexpected(std::move(result.get())));
}

ResultW<std::wstring> TemplateW::RenderAsString(const jinja2::ValuesMap& params)
{
    std::wstring buffer;
    auto result = GetImpl<wchar_t>(m_impl)->Render(buffer, params);

    return !result ? buffer : ResultW<std::wstring>(nonstd::make_unexpected(std::move(result.get())));
}

ResultW<GenericMap> TemplateW::GetMetadata()
{
    return GenericMap();
    // GetImpl<wchar_t>(m_impl)->GetMetadata();
}

ResultW<MetadataInfo<wchar_t>> TemplateW::GetMetadataRaw()
{
    return MetadataInfo<wchar_t>();
    // GetImpl<wchar_t>(m_impl)->GetMetadataRaw();
    ;
}
bool TemplateW::IsEqual(const TemplateW& other) const
{
    return m_impl == other.m_impl;
}

} // namespace jinja2
