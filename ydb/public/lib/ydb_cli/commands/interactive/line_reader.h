#pragma once

#include <memory>
#include <optional>
#include <string>

namespace NYdb::NConsoleClient {

class ILineReader {
public:
    virtual std::optional<std::string> ReadLine() = 0;

    virtual ~ILineReader() = default;
};

std::unique_ptr<ILineReader> CreateLineReader(std::string prompt, std::string historyFilePath);

} // namespace NYdb::NConsoleClient
