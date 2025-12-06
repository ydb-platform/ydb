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
    TSessionRunnerBase(const TSessionSettings& settings, const TInteractiveLogger& log);

    virtual bool Setup(ILineReaderController::TPtr controller) override;

protected:
    static TString PrintCommonHotKeys();

    static TString PrintBold(const TString& text);

    static TString PrintGreen(const TString& text);

protected:
    TInteractiveLogger Log;
    ILineReaderController::TPtr Controller;
    TSessionSettings Settings;
};

} // namespace NYdb::NConsoleClient
