#pragma once

#include <memory>

namespace DB_CHDB
{
class FileLogConsumer;

using ReadBufferFromFileLogPtr = std::shared_ptr<FileLogConsumer>;
}
