#pragma once

#include <ydb/public/lib/ydb_cli/commands/interactive/common/interactive_log.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/session/session_runner_interface.h>

#include <memory>
#include <optional>

namespace NYdb::NConsoleClient {

class ILineReader : public ILineReaderController {
public:
    using TPtr = std::shared_ptr<ILineReader>;

    struct TLine {
        TString Data;
    };

    struct TSwitch {
    };

    virtual std::optional<std::variant<TLine, TSwitch>> ReadLine() = 0;

    virtual void Finish() = 0;

    virtual ~ILineReader() = default;
};

ILineReader::TPtr CreateLineReader(const TDriver& driver, const TString& database, const TInteractiveLogger& log);

} // namespace NYdb::NConsoleClient
