#include "ai_session_runner.h"
#include "session_runner_common.h"

namespace NYdb::NConsoleClient {

namespace NAi {

namespace {

class TAiSessionRunner final : public TSessionRunnerBase {
    using TBase = TSessionRunnerBase;

public:
    TAiSessionRunner(const TAiSessionSettings& settings, const TInteractiveLogger& log)
        : TBase({}, log)
    {}
};

} // anonymous namespace

} // namespace NAi

ISessionRunner::TPtr CreateAiSessionRunner(const TAiSessionSettings& settings, const TInteractiveLogger& log) {
    return std::make_unique<NAi::TAiSessionRunner>(settings, log);
}

} // namespace NYdb::NConsoleClient
