#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/write_session.h>

#include <util/datetime/base.h>

#include <optional>
#include <queue>
#include <unordered_set>
#include <vector>

namespace NYdb::inline Dev::NTopic::NTests {

//! Helper for keyed write session tests: runs event loop and provides continuation tokens.
class TKeyedWriteSessionEventLoop {
public:
    explicit TKeyedWriteSessionEventLoop(std::shared_ptr<IKeyedWriteSession> session);

    //! Block until a continuation token is available or timeout. Calls Run() while waiting.
    //! Returns nullopt on timeout or if session was closed before a token appeared.
    std::optional<TContinuationToken> GetContinuationToken(TDuration timeout);

    bool WaitForAcks(size_t count, TDuration timeout);
    void CheckAcksOrder();

private:
    //! Process all currently available events. Returns true if SessionClosed was seen.
    void Run();

    std::shared_ptr<IKeyedWriteSession> Session_;
    std::queue<TContinuationToken> ReadyTokens_;
    std::unordered_set<ui64> AckedSeqNos_;
    std::vector<ui64> AckOrder_;
    std::mutex Lock_;
};

} // namespace NYdb::inline Dev::NTopic::NTests
