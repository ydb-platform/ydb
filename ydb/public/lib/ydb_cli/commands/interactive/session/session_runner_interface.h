#pragma once

#include <ydb/public/lib/ydb_cli/commands/interactive/common/line_reader.h>

#include <util/generic/string.h>

#include <memory>

namespace NYdb::NConsoleClient {

class ISessionRunner {
public:
    using TPtr = std::shared_ptr<ISessionRunner>;

    virtual ~ISessionRunner() = default;

    virtual ILineReader::TPtr Setup() = 0;

    virtual void HandleLine(const TString& line) = 0;
};

} // namespace NYdb::NConsoleClient
