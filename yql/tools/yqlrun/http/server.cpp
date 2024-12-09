#include "server.h"

#include <library/cpp/json/json_writer.h>

#include <util/generic/map.h>
#include <util/datetime/base.h>
#include <util/datetime/cputimer.h>
#include <util/system/hostname.h>
#include <util/stream/buffer.h>
#include <util/generic/bt_exception.h>


using namespace NYql;
using namespace NHttp;

namespace {

TBlob ToErrorsJson(const TStringBuf& message)
{
    TBufferOutput output;
    NJson::TJsonWriter writer(&output, false);
    writer.OpenMap();

    writer.Write(TStringBuf("errors"));
    {
        writer.OpenArray();
        writer.Write(message);
        writer.CloseArray();
    }

    writer.CloseMap();
    writer.Flush();

    return TBlob::FromBuffer(output.Buffer());
}

///////////////////////////////////////////////////////////////////////////////
// THttpReplier
///////////////////////////////////////////////////////////////////////////////
class THttpReplier: public THttpClientRequestEx
{
public:
    THttpReplier(const TServer* server)
        : Server_(server)
    {
    }

private:
    TString GetRemoteAddr() const {
        NAddr::TOpaqueAddr remoteAddr;
        int rc = getpeername(Socket(), remoteAddr.MutableAddr(), remoteAddr.LenPtr());
        Y_ENSURE(rc == 0, "Can't get remote address");
        return PrintHost(remoteAddr);
    }

    bool Reply(void*) final {
        TSimpleTimer timer;

        if (!ProcessHeaders()) {
            return true;
        }
        RD.Scan();
        TResponse resp;

        const IServlet* servlet = Server_->FindServlet(RD.ScriptName());
        if (servlet != nullptr) {
            try {
                TRequest req{Input(), RD, Buf};

                TStringBuf method = TStringBuf(Input().FirstLine()).Before(' ');
                if (method == TStringBuf("GET")) {
                    servlet->DoGet(req, resp);
                } else if (method == TStringBuf("POST")) {
                    servlet->DoPost(req, resp);
                } else {
                    resp.Code = HTTP_METHOD_NOT_ALLOWED;
                }
            } catch (const THttpError& e) {
                resp.Code = e.GetCode();
                resp.ContentType = TStringBuf("application/json");
                if (resp.Code >= HTTP_BAD_REQUEST) {
                    Cerr << e.what() << Endl;
                    TStringBuf message = e.AsStrBuf().RNextTok(':');
                    if (!message.empty()) {
                        resp.Body = ToErrorsJson(message);
                    }
                }
            } catch (const TWithBackTrace<yexception>& e) {
                Cerr << e.what() << Endl;
                const TBackTrace* bt = e.BackTrace();
                bt->PrintTo(Cerr);
                resp.Code = HTTP_INTERNAL_SERVER_ERROR;
                resp.ContentType = TStringBuf("application/json");
                TStringBuf message = e.AsStrBuf().RNextTok(':');
                if (!message.empty()) {
                    resp.Body = ToErrorsJson(message);
                }
            } catch (const yexception& e) {
                Cerr << e.what() << Endl;
                resp.Code = HTTP_INTERNAL_SERVER_ERROR;
                resp.ContentType = TStringBuf("application/json");
                TStringBuf message = e.AsStrBuf().RNextTok(':');
                if (!message.empty()) {
                    resp.Body = ToErrorsJson(message);
                }
            }
        } else {
            resp.Code = HTTP_NOT_FOUND;
            resp.Body = ToErrorsJson(
                    TString("Unknown path ") + RD.ScriptName());
            resp.ContentType = TStringBuf("application/json");
        }

        resp.Headers.AddHeader(THttpInputHeader("Content-Type", resp.ContentType));
        resp.OutTo(Output());

        Cout << '[' << TInstant::Now().ToStringUpToSeconds() << "] "
             << GetRemoteAddr() << ' '
             << Input().FirstLine() << ' ' << static_cast<int>(resp.Code)
             << " took: " << timer.Get().MilliSeconds() << "ms"
             << Endl;
        return true;
    }

private:
    const TServer* Server_;
};

THttpServerOptions CreateOptions(const TServerConfig& config)
{
    THttpServerOptions opts;

    if (config.IsBind(TBind::OnLocal)) {
        opts.AddBindAddress("localhost", config.GetPort());
    }

    if (config.IsBind(TBind::OnRemote)) {
        opts.AddBindAddress(HostName(), config.GetPort());
    }

    opts.SetPort(config.GetPort());
    opts.SetThreads(4);
    opts.SetMaxQueueSize(40);

    return opts;
}

} // namspace


namespace NYql {
namespace NHttp {

void TServerConfig::InitCliOptions(NLastGetopt::TOpts& opts)
{
    opts.AddLongOption("port", "listening port")
            .StoreResult<ui16>(&Port_)
            .DefaultValue("3000");
    opts.AddLongOption("local", "bind on local interface").NoArgument();
    opts.AddLongOption("remote", "bind on remote interface").NoArgument();
    opts.AddLongOption("assets", "path to folder with web intrface files")
            .StoreResult<TString>(&AssetsPath_).DefaultValue(AssetsPath_);
}
void TServerConfig::ParseFromCli(NLastGetopt::TOptsParseResult& res)
{
    if (res.Has("local")) {
        Bind(TBind::OnLocal);
    }

    if (res.Has("remote")) {
        Bind(TBind::OnRemote);
    }

    if (Bind_ == 0) {
        Bind(TBind::OnLocal);
    }
}

///////////////////////////////////////////////////////////////////////////////
// TServer
///////////////////////////////////////////////////////////////////////////////
TServer::TServer(const TServerConfig& config)
    : THttpServer(this, CreateOptions(config))
{
    Cout << "Start server listening on\n";
    if (config.IsBind(TBind::OnLocal)) {
        Cout << "http://localhost:" << config.GetPort() << "/\n";
    }
    if (config.IsBind(TBind::OnRemote)) {
        Cout << "http://" << HostName() << ':' << config.GetPort() << "/\n";
    }
    Cout << Endl;
}

TServer::~TServer()
{
    for (auto& it: Servlets_) {
        delete it.second;
    }
}

void TServer::RegisterServlet(const TString& path, TAutoPtr<IServlet> sp)
{
    Cout << "Registered servlet on " << path << Endl;

    IServlet* servlet = sp.Release();
    Servlets_.push_back(std::pair<TString, IServlet*>(path, servlet));
}

TClientRequest* TServer::CreateClient()
{
    return new THttpReplier(this);
}

const IServlet* TServer::FindServlet(TStringBuf path) const
{
    for (const auto& servlet: Servlets_) {
        const TString& urlPrefix = servlet.first;
        if (path.StartsWith(urlPrefix))
            return servlet.second;
    }

    return nullptr;
}

TVector<TString> TServer::GetUrlMappings() const
{
    TVector<TString> urls;
    urls.reserve(Servlets_.size());
    for (const auto& s: Servlets_) {
        urls.push_back(s.first);
    }
    return urls;
}

} // namspace NWeb
} // namspace NYql
