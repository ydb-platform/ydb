#include <jinja2cpp/filesystem_handler.h>
#include <jinja2cpp/string_helpers.h>

#include <boost/filesystem/path.hpp>
#include <boost/filesystem/operations.hpp>

#include <sstream>
#include <fstream>

namespace jinja2
{

using TargetFileStream = std::variant<CharFileStreamPtr*, WCharFileStreamPtr*>;

struct FileContentConverter
{
    void operator() (const std::string& content, CharFileStreamPtr* sPtr) const
    {
        sPtr->reset(new std::istringstream(content));
    }

    void operator() (const std::wstring& content, WCharFileStreamPtr* sPtr) const
    {
        sPtr->reset(new std::wistringstream(content));
    }
    void operator() (const std::wstring&, CharFileStreamPtr*) const
    {
//        CharFileStreamPtr stream(new std::istringstream(content), [](std::istream* s) {delete static_cast<std::istringstream>(s);});
//        std::swap(*sPtr, stream);
    }

    void operator() (const std::string&, WCharFileStreamPtr*) const
    {
//        WCharFileStreamPtr stream(new std::wistringstream(content), [](std::wistream* s) {delete static_cast<std::wistringstream>(s);});
//        std::swap(*sPtr, stream);
    }
};

void MemoryFileSystem::AddFile(std::string fileName, std::string fileContent)
{
    m_filesMap[std::move(fileName)] = FileContent{std::move(fileContent), {}};
}

void MemoryFileSystem::AddFile(std::string fileName, std::wstring fileContent)
{
    m_filesMap[std::move(fileName)] = FileContent{ {}, std::move(fileContent) };
}

CharFileStreamPtr MemoryFileSystem::OpenStream(const std::string& name) const
{
    CharFileStreamPtr result(nullptr, [](std::istream* s) {delete static_cast<std::istringstream*>(s);});
    auto p = m_filesMap.find(name);
    if (p == m_filesMap.end())
        return result;

    auto& content = p->second;

    if (!content.narrowContent && !content.wideContent)
        return result;

    if (!content.narrowContent)
        content.narrowContent = ConvertString<std::string>(content.wideContent.value());

    result.reset(new std::istringstream(content.narrowContent.value()));

    return result;
}

WCharFileStreamPtr MemoryFileSystem::OpenWStream(const std::string& name) const
{
    WCharFileStreamPtr result(nullptr, [](std::wistream* s) {delete static_cast<std::wistringstream*>(s);});
    auto p = m_filesMap.find(name);
    if (p == m_filesMap.end())
        return result;

    auto& content = p->second;

    if (!content.narrowContent && !content.wideContent)
        return result;

    if (!content.wideContent)
        content.wideContent = ConvertString<std::wstring>(content.narrowContent.value());

    result.reset(new std::wistringstream(content.wideContent.value()));

    return result;
}
std::optional<std::chrono::system_clock::time_point> MemoryFileSystem::GetLastModificationDate(const std::string&) const
{
    return std::optional<std::chrono::system_clock::time_point>();
}

bool MemoryFileSystem::IsEqual(const IComparable& other) const
{
    auto* ptr = dynamic_cast<const MemoryFileSystem*>(&other);
    if (!ptr)
        return false;
    return m_filesMap == ptr->m_filesMap;
}

RealFileSystem::RealFileSystem(std::string rootFolder)
    : m_rootFolder(std::move(rootFolder))
{

}

std::string RealFileSystem::GetFullFilePath(const std::string& name) const
{
    boost::filesystem::path root(m_rootFolder);
    root /= name;
    return root.string();
}

CharFileStreamPtr RealFileSystem::OpenStream(const std::string& name) const
{
    auto filePath = GetFullFilePath(name);

    CharFileStreamPtr result(new std::ifstream(filePath), [](std::istream* s) {delete static_cast<std::ifstream*>(s);});
    if (result->good())
        return result;

    return CharFileStreamPtr(nullptr, [](std::istream*){});
}

WCharFileStreamPtr RealFileSystem::OpenWStream(const std::string& name) const
{
    auto filePath = GetFullFilePath(name);

    WCharFileStreamPtr result(new std::wifstream(filePath), [](std::wistream* s) {delete static_cast<std::wifstream*>(s);});
    if (result->good())
        return result;

    return WCharFileStreamPtr(nullptr, [](std::wistream*){;});
}
std::optional<std::chrono::system_clock::time_point> RealFileSystem::GetLastModificationDate(const std::string& name) const
{
    boost::filesystem::path root(m_rootFolder);
    root /= name;

    auto modify_time = boost::filesystem::last_write_time(root);

    return std::chrono::system_clock::from_time_t(modify_time);
}
CharFileStreamPtr RealFileSystem::OpenByteStream(const std::string& name) const
{
    auto filePath = GetFullFilePath(name);

    CharFileStreamPtr result(new std::ifstream(filePath, std::ios_base::binary), [](std::istream* s) {delete static_cast<std::ifstream*>(s);});
    if (result->good())
        return result;

    return CharFileStreamPtr(nullptr, [](std::istream*){});
}

bool RealFileSystem::IsEqual(const IComparable& other) const
{
    auto* ptr = dynamic_cast<const RealFileSystem*>(&other);
    if (!ptr)
        return false;
    return m_rootFolder == ptr->m_rootFolder;
}

} // namespace jinja2
