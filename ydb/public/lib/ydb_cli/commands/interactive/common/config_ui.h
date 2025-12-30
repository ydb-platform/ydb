#pragma once

#include "line_reader.h"

#include <memory>

namespace NYdb::NConsoleClient {

// Configuration UI for interactive mode
class TConfigUI {
public:
    struct TContext {
        ILineReader::TPtr LineReader;
    };

    // Run the config UI, returns true if user made changes
    static bool Run(const TContext& ctx);

private:
    // Individual settings dialogs
    static bool RunEnableHints(const TContext& ctx);
    static bool RunChooseTheme(const TContext& ctx);
    static bool RunCloneTheme(const TContext& ctx);
    static bool RunColorsMode(const TContext& ctx);
};

} // namespace NYdb::NConsoleClient
