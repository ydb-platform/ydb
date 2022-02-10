#pragma once

#include <library/cpp/messagebus/rain_check/core/task.h>

#include <library/cpp/http/misc/httpcodes.h>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/system/defaults.h>
#include <util/system/yassert.h>

class THttpHeaders;

namespace NNeh {
    class IProtocol;
    struct TResponse;
}

namespace NRainCheck {
    class THttpCallback;
    class THttpClientService;

    class THttpFuture: public TSubtaskCompletion {
    public:
        enum EError {
            NoError = 0,

            CantResolveNameError = 1,
            BadHttpCodeError = 2,

            OtherError = 100
        };

    private:
        friend class THttpCallback;
        friend class THttpClientService;

    public:
        THttpFuture();
        ~THttpFuture() override;

        bool HasHttpCode() const;
        bool HasResponseBody() const;

        ui32 GetHttpCode() const;
        TString GetResponseBody() const;

        bool HasError() const;
        EError GetErrorCode() const;
        TString GetErrorDescription() const;

    private:
        void SetDoneAndSchedule(TAutoPtr<NNeh::TResponse> response);
        void SetFail(EError errorCode, const TStringBuf& errorDescription);

    private:
        TTaskRunnerBase* Task;
        TMaybe<HttpCodes> HttpCode;
        THolder<NNeh::TResponse> Response;
        EError ErrorCode;
        TString ErrorDescription;
    };

    class THttpClientService {
    public:
        THttpClientService();
        virtual ~THttpClientService();

        void Send(const TString& request, THttpFuture* future);
        void SendPost(TString addr, const TString& data, const THttpHeaders& headers, THttpFuture* future);

    private:
        NNeh::IProtocol* const GetProtocol;
        NNeh::IProtocol* const FullProtocol;
    };

}
