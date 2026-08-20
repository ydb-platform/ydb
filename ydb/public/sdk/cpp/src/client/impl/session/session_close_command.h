#pragma once

#include <string_view>

namespace NYdb::inline Dev {

class ISessionClient;
class TKqpSessionCommon;
class TStatus;

namespace NSessionPool {

class TSessionCloseCommand {
public:
    using TTransition = bool (TKqpSessionCommon::*)();

    constexpr TSessionCloseCommand(std::string_view reason, TTransition transition)
        : Reason_(reason)
        , Transition_(transition)
    {
    }

    bool Execute(TKqpSessionCommon& session, ISessionClient* client) const;

private:
    const std::string_view Reason_;
    const TTransition Transition_;
};

namespace NSessionCloseCommands {

extern const TSessionCloseCommand PoolIdleTimeout;
extern const TSessionCloseCommand PoolGracefulShutdown;
extern const TSessionCloseCommand ClientTimeout;
extern const TSessionCloseCommand ClientCancelled;
extern const TSessionCloseCommand AttachClosed;
extern const TSessionCloseCommand TransportError;
extern const TSessionCloseCommand NodeShutdown;
extern const TSessionCloseCommand SessionShutdown;
extern const TSessionCloseCommand BadSession;
extern const TSessionCloseCommand SessionBusy;

const TSessionCloseCommand* FromStatus(const TStatus& status);

} // namespace NSessionCloseCommands
} // namespace NSessionPool
} // namespace NYdb
