#pragma once

#include <kikimr/persqueue/sdk/deprecated/cpp/v2/types.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/responses.h>

#include <library/cpp/threading/future/future.h>

#include <deque>

namespace NPersQueue {

struct TChannelInfo {
    std::shared_ptr<grpc::Channel> Channel;
    ui64 ProxyCookie;
};

class TChannel;
using TChannelPtr = TIntrusivePtr<TChannel>;

class TPQLibPrivate;

struct TChannelHolder {
    TChannelPtr ChannelPtr;
    NThreading::TFuture<TChannelInfo> ChannelInfo;
};

class TChannelImpl;
using TChannelImplPtr = TIntrusivePtr<TChannelImpl>;

class TChannel: public TAtomicRefCount<TChannel> {
public:
    friend class TPQLibPrivate;

    NThreading::TFuture<TChannelInfo> GetChannel();

    ~TChannel();

    void Start();

private:
    TChannel(const TServerSetting& server, const std::shared_ptr<ICredentialsProvider>& credentialsProvider, TPQLibPrivate* pqLib,
             TIntrusivePtr<ILogger> logger = nullptr, bool preferLocalProxy = false);
    TChannel(const TProducerSettings& settings, TPQLibPrivate* pqLib,
             TIntrusivePtr<ILogger> logger = nullptr, bool preferLocalProxy = false);
    void MakeImpl(const TServerSetting& server, const TCredProviderPtr&, TPQLibPrivate* pqLib,
                  TIntrusivePtr<ILogger> logger = nullptr, bool preferLocalProxy = false);
    TChannelImplPtr Impl;
};

}
