#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oidc/credentials.h>

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/http/misc/parsed_request.h>
#include <library/cpp/http/server/http.h>
#include <library/cpp/http/server/response.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <condition_variable>
#include <deque>
#include <mutex>

class TOidcTestServer: public THttpServer::ICallBack {
public:
    struct TReply {
        HttpCodes Status = HTTP_OK;
        TString Body;
    };

    struct TRequestInfo {
        TString Path;
        TString Method;
        TCgiParameters Form;
        TString Authorization;
    };

    TOidcTestServer()
        : Options(Ports.GetPort())
        , Server(this, Options)
    {
        Server.Start();
    }

    ~TOidcTestServer() override {
        Server.Stop();
    }

    std::string Issuer() const {
        return "http://localhost:" + std::to_string(Options.Port) + "/realm";
    }

    NYdb::TOidcConfig ClientConfig() const {
        NYdb::TOidcConfig config;
        config.Issuer = Issuer();
        config.AllowInsecureHttp(true).TokenEndpointAuthMethod("client_secret_post");
        config.FlowConfig = NYdb::TClientOidcConfig{"client", "secret +&", {"read", "write"}};
        return config;
    }

    void Enqueue(TString body, HttpCodes status) {
        std::lock_guard lock(Mutex);
        Replies.push_back({status, std::move(body)});
    }

    void BlockTokenRepliesUntil(NThreading::TFuture<void> released) {
        std::lock_guard lock(Mutex);
        TokenReplyGate = std::move(released);
    }

    std::vector<TRequestInfo> Requests() const {
        std::lock_guard lock(Mutex);
        return Recorded;
    }

    size_t DiscoveryCount() const {
        std::lock_guard lock(Mutex);
        return Discoveries;
    }

    bool WaitRequests(size_t count) {
        std::unique_lock lock(Mutex);
        return Changed.wait_for(lock, std::chrono::seconds(10), [&] { return Recorded.size() >= count; });
    }

    class TRequest: public TRequestReplier {
    public:
        explicit TRequest(TOidcTestServer& server)
            : Server(server)
        {
        }

        bool DoReply(const TReplyParams& params) override {
            const TParsedHttpFull parsed(params.Input.FirstLine());
            const TString body = params.Input.ReadAll();
            TReply reply;
            NThreading::TFuture<void> replyGate;
            {
                std::lock_guard lock(Server.Mutex);
                if (parsed.Path == "/realm/.well-known/openid-configuration") {
                    ++Server.Discoveries;
                    NJson::TJsonValue metadata;
                    metadata["issuer"] = Server.Issuer();
                    metadata["token_endpoint"] = Server.Issuer() + "/token";
                    metadata["device_authorization_endpoint"] = Server.Issuer() + "/device";
                    reply.Body = NJson::WriteJson(metadata, false);
                } else {
                    TRequestInfo request{TString(parsed.Path), TString(parsed.Method), TCgiParameters(body), {}};
                    for (const auto& header : params.Input.Headers()) {
                        if (header.Name() == "Authorization") {
                            request.Authorization = header.Value();
                        }
                    }
                    Server.Recorded.push_back(std::move(request));
                    if (parsed.Path == "/realm/token") {
                        replyGate = Server.TokenReplyGate;
                    }
                    if (Server.Replies.empty()) {
                        reply = {HTTP_BAD_REQUEST, R"({"error":"unexpected_request"})"};
                    } else {
                        reply = std::move(Server.Replies.front());
                        Server.Replies.pop_front();
                    }
                    Server.Changed.notify_all();
                }
            }
            if (replyGate.Initialized()) {
                replyGate.Wait();
            }
            THttpResponse response(reply.Status);
            response.SetContent(reply.Body);
            response.OutTo(params.Output);
            return true;
        }

    private:
        TOidcTestServer& Server;
    };

    TClientRequest* CreateClient() override {
        return new TRequest(*this);
    }

private:
    TPortManager Ports;
    THttpServer::TOptions Options;
    mutable std::mutex Mutex;
    std::condition_variable Changed;
    std::deque<TReply> Replies;
    std::vector<TRequestInfo> Recorded;
    size_t Discoveries = 0;
    NThreading::TFuture<void> TokenReplyGate;
    THttpServer Server;
};
