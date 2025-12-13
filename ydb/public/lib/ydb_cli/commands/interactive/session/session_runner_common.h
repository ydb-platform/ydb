#pragma once

#include "session_runner_interface.h"

#include <ydb/public/lib/ydb_cli/commands/interactive/common/interactive_log.h>

#include <util/stream/output.h>

#include <library/cpp/colorizer/colors.h>

namespace NYdb::NConsoleClient {

class TSessionRunnerBase : public ISessionRunner {
protected:
    inline const static NColorizer::TColors Colors = NColorizer::AutoColors(Cout);

public:
    TSessionRunnerBase(const TLineReaderSettings& settings, const TInteractiveLogger& log);

    virtual ILineReader::TPtr Setup() override;

protected:
    static TString PrintCommonHotKeys();

protected:
    TInteractiveLogger Log;
    ILineReader::TPtr LineReader;
    TLineReaderSettings Settings;
};

} // namespace NYdb::NConsoleClient
