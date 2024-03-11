#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace NYdb {
namespace NConsoleClient {

struct Suggest
{
    std::vector<std::string> Words;
};

class ILineReader {
public:
    virtual std::optional<std::string> ReadLine() = 0;

    virtual ~ILineReader() = default;

};

std::unique_ptr<ILineReader> CreateLineReader(std::string prompt, std::string historyFilePath, Suggest suggestions = {});

}
}
