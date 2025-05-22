#pragma once

#include "servlet.h"

#include <library/cpp/http/server/http_ex.h>
#include <library/cpp/getopt/last_getopt.h>

#include <util/generic/string.h>
#include <util/system/types.h>


namespace NYql {
namespace NHttp {

///////////////////////////////////////////////////////////////////////////////
// TServerConfig
///////////////////////////////////////////////////////////////////////////////
struct TBind {
    enum {
        OnLocal = 0x01,
        OnRemote = 0x02,
    };
};

class TServerConfig
{
public:
    TServerConfig()
        : Bind_(0)
        , Port_(0)
    {
    }

    inline TServerConfig& Bind(ui32 on) {
        Bind_ |= on;
        return *this;
    }

    inline bool IsBind(ui32 on) const {
        return Bind_ & on;
    }

    inline TServerConfig& SetPort(ui16 port) {
        Port_ = port;
        return *this;
    }

    inline ui16 GetPort() const {
        return Port_;
    }

    inline TServerConfig& SetAssetsPath(const TString& path) {
        AssetsPath_ = path;
        return *this;
    }

    inline const TString& GetAssetsPath() const {
        return AssetsPath_;
    }

    void InitCliOptions(NLastGetopt::TOpts& opts);
    void ParseFromCli(NLastGetopt::TOptsParseResult& res);

private:
    ui32 Bind_;
    ui16 Port_;
    TString AssetsPath_;
};

///////////////////////////////////////////////////////////////////////////////
// TServer
///////////////////////////////////////////////////////////////////////////////
class TServer:
        public THttpServer, public THttpServer::ICallBack,
        private TNonCopyable
{
public:
    TServer(const TServerConfig& config);
    ~TServer();

    void RegisterServlet(const TString& path, TAutoPtr<IServlet> sp);
    const IServlet* FindServlet(TStringBuf path) const;
    TVector<TString> GetUrlMappings() const;

private:
    TClientRequest* CreateClient() override final;

private:
    TVector<std::pair<TString, IServlet*>> Servlets_;
};

} // namspace NNttp
} // namspace NYql
