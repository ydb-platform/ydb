#pragma once

#include <memory>
#include <optional>
#include <string>

#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb::NConsoleClient {

class ILineReader {
public:
    virtual std::optional<std::string> ReadLine() = 0;
    virtual void ClearHints() = 0;

    virtual ~ILineReader() = default;
};

std::unique_ptr<ILineReader> CreateLineReader(
    std::string prompt, std::string historyFilePath, TClientCommand::TConfig& config);

} // namespace NYdb::NConsoleClient
