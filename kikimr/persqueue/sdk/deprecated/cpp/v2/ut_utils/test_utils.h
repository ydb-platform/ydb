#pragma once
#include <util/generic/ptr.h>
#include <util/generic/size_literals.h>
#include <library/cpp/threading/chunk_queue/queue.h>
#include <util/generic/overloaded.h>
#include <library/cpp/testing/unittest/registar.h>

#include <kikimr/persqueue/sdk/deprecated/cpp/v2/ut_utils/sdk_test_setup.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/persqueue.h>

namespace NPersQueue {

using namespace NThreading;
using namespace NYdb::NPersQueue;
using namespace NKikimr;
using namespace NKikimr::NPersQueueTests;
//using namespace NPersQueue::V1;

template <class TPQLibObject>
void DestroyAndWait(THolder<TPQLibObject>& object) {
    if (object) {
        auto isDead = object->IsDead();
        object = nullptr;
        isDead.GetValueSync();
    }
}

inline bool GrpcV1EnabledByDefault() {
    static const bool enabled = std::getenv("PERSQUEUE_GRPC_API_V1_ENABLED");
    return enabled;
}

class TCallbackCredentialsProvider : public ICredentialsProvider {
    std::function<void(NPersQueue::TCredentials*)> Callback;
public:
    TCallbackCredentialsProvider(std::function<void(NPersQueue::TCredentials*)> callback)
    : Callback(std::move(callback))
    {}

    void FillAuthInfo(NPersQueue::TCredentials* authInfo) const {
        Callback(authInfo);
    }
};

struct TWriteResult {
    bool Ok = false;
    // No acknowledgement is expected from a writer under test
    bool NoWait = false;
    TString ResponseDebugString = TString();
};

struct TAcknowledgableMessage {
    TString Value;
    ui64 SequenceNumber;
    TInstant CreatedAt;
    TPromise<TWriteResult> AckPromise;
};

class IClientEventLoop {
protected:
    std::atomic_bool MayStop;
    std::atomic_bool MustStop;
    bool Stopped = false;
    std::unique_ptr<TThread> Thread;
    TLog Log;

public:
    IClientEventLoop()
    : MayStop()
    , MustStop()
    , MessageBuffer()
    {}

    void AllowStop() {
        MayStop = true;
    }

    void WaitForStop() {
        if (!Stopped) {
            Log << TLOG_INFO << "Wait for writer to die on itself";
            Thread->Join();
            Log << TLOG_INFO << "Client write event loop stopped";
        }
        Stopped = true;
    }

    virtual ~IClientEventLoop() {
        MustStop = true;
        if (!Stopped) {
            Log << TLOG_INFO << "Wait for client write event loop to stop";
            Thread->Join();
            Log << TLOG_INFO << "Client write event loop stopped";
        }
        Stopped = true;
    }

    TManyOneQueue<TAcknowledgableMessage> MessageBuffer;

};

} // namespace NPersQueue
