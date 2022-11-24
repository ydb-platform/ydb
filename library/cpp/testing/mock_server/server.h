#pragma once

#include <library/cpp/http/misc/parsed_request.h>
#include <library/cpp/http/server/http.h>
#include <library/cpp/http/server/response.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/system/event.h>

#include <functional>

class TAutoEvent;
class TThread;

namespace NMock {
    class TMockServer {
    public:
        using TGenerator = std::function<TRequestReplier*()>;

        TMockServer(ui16 port, TGenerator generator);
        TMockServer(const THttpServerOptions& options, TGenerator generator);

        ~TMockServer();

        size_t GetClientCount() const;
        void SetGenerator(TGenerator generator);

    private:
        static void* Worker(void* arg);

        class TCallBack;

        THolder<TCallBack> Cb_;
        THolder<THttpServer> Server_;
        THolder<TThread> Thread_;
        THolder<TAutoEvent> Ev_;
    };

    class TPong: public TRequestReplier {
    public:
        bool DoReply(const TReplyParams& params) override {
            const TParsedHttpFull parsed(params.Input.FirstLine());

            const HttpCodes code = parsed.Path == "/ping" ? HTTP_OK : HTTP_NOT_FOUND;

            THttpResponse resp(code);
            resp.OutTo(params.Output);

            return true;
        }
    };
}
