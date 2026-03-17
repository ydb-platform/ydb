#pragma once

#include <memory>

namespace CHDBPoco::Util { class AbstractConfiguration; }

namespace DB_CHDB
{

class IAsynchronousReader;

enum class FilesystemReaderType : uint8_t
{
    SYNCHRONOUS_LOCAL_FS_READER,
    ASYNCHRONOUS_LOCAL_FS_READER,
    ASYNCHRONOUS_REMOTE_FS_READER,
};

IAsynchronousReader & getThreadPoolReader(FilesystemReaderType type);

std::unique_ptr<IAsynchronousReader> createThreadPoolReader(
    FilesystemReaderType type,
    const CHDBPoco::Util::AbstractConfiguration & config);

}
