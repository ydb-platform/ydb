#ifndef JINJA2CPP_FILESYSTEM_HANDLER_H
#define JINJA2CPP_FILESYSTEM_HANDLER_H

#include "config.h"

#include <jinja2cpp/utils/i_comparable.h>

#include <optional>
#include <variant>

#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>

namespace jinja2
{

template<typename CharT>
using FileStreamPtr = std::unique_ptr<std::basic_istream<CharT>, void (*)(std::basic_istream<CharT>*)>;
using CharFileStreamPtr = FileStreamPtr<char>;
using WCharFileStreamPtr = FileStreamPtr<wchar_t>;

/*!
 * \brief Generic interface to filesystem handlers (loaders)
 *
 * This interface should be implemented in order to provide custom file system handler. Interface provides most-common methods which are called by
 * the template environment to load the particular template. `OpenStream` methods return the unique pointer to the generic `istream` object implementation.
 * So, the exact type (ex. `ifstream`, `istringstream` etc.) of input stream is unspecified. In order to delete stream object correctly returned pointer
 * provide the custom deleter which should properly delete the stream object.
 */
class JINJA2CPP_EXPORT IFilesystemHandler : public IComparable
{
public:
    //! Destructor
    virtual ~IFilesystemHandler() = default;

    /*!
     * \brief Method is called to open the file with the specified name in 'narrow-char' mode.
     *
     * Method should return unique pointer to the std::istream object with custom deleter (\ref CharFileStreamPtr) . Deleter should properly delete pointee
     * stream object.
     *
     * @param name Name of the file to open
     * @return Opened stream object or empty pointer in case of any error
     */
    virtual CharFileStreamPtr OpenStream(const std::string& name) const = 0;
    /*!
     * \brief Method is called to open the file with the specified name in 'wide-char' mode.
     *
     * Method should return unique pointer to the std::wistream object with custom deleter (\ref WCharFileStreamPtr) . Deleter should properly delete pointee
     * stream object.
     *
     * @param name Name of the file to open
     * @return Opened stream object or empty pointer in case of any error
     */
    virtual WCharFileStreamPtr OpenWStream(const std::string& name) const = 0;
    /*!
     * \brief Method is called to obtain the modification date of the specified file (if applicable)
     *
     * If the underlaying filesystem supports retrival of the last modification date of the file this method should return this date when called. In other
     * case it should return the empty optional object. Main purpose of this method is to help templates loader to determine the necessity of cached template
     * reload
     *
     * @param name Name of the file to get the last modification date
     * @return Last modification date (if applicable) or empty optional object otherwise
     */
    virtual std::optional<std::chrono::system_clock::time_point> GetLastModificationDate(const std::string& name) const = 0;
};

using FilesystemHandlerPtr = std::shared_ptr<IFilesystemHandler>;

/*!
 * \brief Filesystem handler for files stored in memory
 *
 * This filesystem handler implements the simple dictionary object which maps name of the file to it's content. New files can be added by \ref AddFile
 * methods. Content of the files automatically converted to narrow/wide strings representation if necessary.
 */
class JINJA2CPP_EXPORT MemoryFileSystem : public IFilesystemHandler
{
public:
    /*!
     * \brief Add new narrow-char "file" to the filesystem handler
     *
     * Adds new file entry to the internal dictionary object or overwrite the existing one. New entry contains the specified content of the file
     *
     * @param fileName Name of the file to add
     * @param fileContent Content of the file to add
     */
    void AddFile(std::string fileName, std::string fileContent);
    /*!
     * \brief Add new wide-char "file" to the filesystem handler
     *
     * Adds new file entry to the internal dictionary object or overwrite the existing one. New entry contains the specified content of the file
     *
     * @param fileName Name of the file to add
     * @param fileContent Content of the file to add
     */
    void AddFile(std::string fileName, std::wstring fileContent);

    CharFileStreamPtr OpenStream(const std::string& name) const override;
    WCharFileStreamPtr OpenWStream(const std::string& name) const override;
    std::optional<std::chrono::system_clock::time_point> GetLastModificationDate(const std::string& name) const override;

    /*!
     * \brief Compares to an object of the same type
     *
     * return true if equal
     */
    bool IsEqual(const IComparable& other) const override;
private:
    struct FileContent
    {
        std::optional<std::string> narrowContent;
        std::optional<std::wstring> wideContent;
        bool operator==(const FileContent& other) const
        {
            if (narrowContent != other.narrowContent)
                return false;
            return wideContent == other.wideContent;
        }
        bool operator!=(const FileContent& other) const
        {
            return !(*this == other);
        }
    };
    mutable std::unordered_map<std::string, FileContent> m_filesMap;
};

/*!
 * \brief Filesystem handler for files stored on the filesystem
 *
 * This filesystem handler is an interface to the real file system. Root directory for file name mapping provided as a constructor argument. Each name (path) of
 * the file to open is appended to the root directory path and then passed to the stream open methods.
 */
class JINJA2CPP_EXPORT RealFileSystem : public IFilesystemHandler
{
public:
    /*!
     * \brief Initializing/default constructor
     *
     * @param rootFolder Path to the root folder. This path is used as a root for every opened file
     */
    explicit RealFileSystem(std::string rootFolder = ".");

    /*!
     * \brief Reset path to the root folder to the new value
     *
     * @param newRoot New path to the root folder
     */
    void SetRootFolder(std::string newRoot)
    {
        m_rootFolder = std::move(newRoot);
    }

    /*!
     * \brief Get path to the current root folder
     *
     * @return
     */
    const std::string& GetRootFolder() const
    {
      return m_rootFolder;
    }
    /*!
     * \brief Get full path to the specified file.
     *
     * Appends specified name of the file to the current root folder and returns it
     *
     * @param name Name of the file to get path to
     * @return Full path to the file
     */
    std::string GetFullFilePath(const std::string& name) const;

    CharFileStreamPtr OpenStream(const std::string& name) const override;
    WCharFileStreamPtr OpenWStream(const std::string& name) const override;
    /*!
     * \brief Open the specified file as a binary stream
     *
     * Opens the specified file in the binary mode (instead of text).
     *
     * @param name Name of the file to get the last modification date
     * @return Last modification date (if applicable) or empty optional object otherwise
     */
    CharFileStreamPtr OpenByteStream(const std::string& name) const;
    std::optional<std::chrono::system_clock::time_point> GetLastModificationDate(const std::string& name) const override;

    /*!
     * \brief Compares to an object of the same type
     *
     * return true if equal
     */
    bool IsEqual(const IComparable& other) const override;

private:
    std::string m_rootFolder;
};
} // namespace jinja2

#endif // JINJA2CPP_FILESYSTEM_HANDLER_H
