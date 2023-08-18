#pragma once
#include <util/system/types.h>
#include <util/generic/ptr.h>
#include <util/stream/input.h>
#include <util/stream/output.h>
#include <util/thread/pool.h>
#include <util/network/pair.h>
#include <util/network/poller.h>

#include "ldap_defines.h"

class TInetStreamSocket;
class TStreamSocket;

namespace LdapMock {

class TLdapSimpleServer {
public:
    using TRequestHandler = std::function<void(TAtomicSharedPtr<TStreamSocket> socket)>;

public:
    TLdapSimpleServer(ui16 port, const TLdapMockResponses& responses);
    ~TLdapSimpleServer();

    void Stop();

    int GetPort() const;
    TString GetAddress() const;

    void SetBindResponse(const std::pair<TBindRequestInfo, TBindResponseInfo>& response);
    void SetSearchReasponse(const std::pair<TSearchRequestInfo, TSearchResponseInfo>& response);

private:
    const int Port;
    THolder<IThreadPool> ThreadPool;
    THolder<IThreadFactory::IThread> ListenerThread;
    THolder<TInetStreamSocket> SendFinishSocket;

    TLdapMockResponses Responses;
};

}
