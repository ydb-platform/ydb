#pragma once

#include "interactive_log.h"

#include <ydb/public/lib/ydb_cli/common/command.h>

#include <memory>
#include <optional>

namespace NYdb::NConsoleClient {

class ILineReader {
public:
    using TPtr = std::unique_ptr<ILineReader>;

    virtual std::optional<TString> ReadLine() = 0;

    virtual void Finish() = 0;

    virtual ~ILineReader() = default;
};

ILineReader::TPtr CreateLineReader(const TString& prompt, const TString& historyFilePath, const TDriver& driver, const TString& database, const TInteractiveLogger& log);

} // namespace NYdb::NConsoleClient
