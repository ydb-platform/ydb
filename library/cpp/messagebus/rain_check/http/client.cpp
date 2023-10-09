#include "client.h"

#include "http_code_extractor.h"

#include <library/cpp/http/io/stream.h>
#include <library/cpp/neh/factory.h>
#include <library/cpp/neh/http_common.h>
#include <library/cpp/neh/location.h>
#include <library/cpp/neh/neh.h>

#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>
#include <util/network/socket.h>
#include <util/stream/str.h>

namespace NRainCheck {
    class THttpCallback: public NNeh::IOnRecv {
    public:
        THttpCallback(NRainCheck::THttpFuture* future)
            : Future(future)
        {
            Y_ABORT_UNLESS(!!future, "future is NULL");
        }

        void OnRecv(NNeh::THandle& handle) override {
            THolder<THttpCallback> self(this);
            NNeh::TResponseRef response = handle.Get();
            Future->SetDoneAndSchedule(response);
        }

    private:
        NRainCheck::THttpFuture* const Future;
    };

    THttpFuture::THttpFuture()
        : Task(nullptr)
        , ErrorCode(THttpFuture::NoError)
    {
    }

    THttpFuture::~THttpFuture() {
    }

    bool THttpFuture::HasError() const {
        return (ErrorCode != THttpFuture::NoError);
    }

    THttpFuture::EError THttpFuture::GetErrorCode() const {
        return ErrorCode;
    }

    TString THttpFuture::GetErrorDescription() const {
        return ErrorDescription;
    }

    THttpClientService::THttpClientService()
        : GetProtocol(NNeh::ProtocolFactory()->Protocol("http"))
        , FullProtocol(NNeh::ProtocolFactory()->Protocol("full"))
    {
        Y_ABORT_UNLESS(!!GetProtocol, "GET protocol is NULL.");
        Y_ABORT_UNLESS(!!FullProtocol, "POST protocol is NULL.");
    }

    THttpClientService::~THttpClientService() {
    }

    void THttpClientService::SendPost(TString addr, const TString& data, const THttpHeaders& headers, THttpFuture* future) {
        Y_ABORT_UNLESS(!!future, "future is NULL.");

        TTaskRunnerBase* current = TTaskRunnerBase::CurrentTask();
        future->SetRunning(current);
        future->Task = current;

        THolder<THttpCallback> callback(new THttpCallback(future));
        NNeh::TServiceStatRef stat;
        try {
            NNeh::TMessage msg(addr.replace(0, NNeh::TParsedLocation(addr).Scheme.size(), "post"), data);
            TStringStream headersText;
            headers.OutTo(&headersText);
            NNeh::NHttp::MakeFullRequest(msg, headersText.Str(), TString());
            FullProtocol->ScheduleRequest(msg, callback.Get(), stat);
            Y_UNUSED(callback.Release());
        } catch (const TNetworkResolutionError& err) {
            future->SetFail(THttpFuture::CantResolveNameError, err.AsStrBuf());
        } catch (const yexception& err) {
            future->SetFail(THttpFuture::OtherError, err.AsStrBuf());
        }
    }

    void THttpClientService::Send(const TString& request, THttpFuture* future) {
        Y_ABORT_UNLESS(!!future, "future is NULL.");

        TTaskRunnerBase* current = TTaskRunnerBase::CurrentTask();
        future->SetRunning(current);
        future->Task = current;

        THolder<THttpCallback> callback(new THttpCallback(future));
        NNeh::TServiceStatRef stat;
        try {
            GetProtocol->ScheduleRequest(NNeh::TMessage::FromString(request),
                                         callback.Get(),
                                         stat);
            Y_UNUSED(callback.Release());
        } catch (const TNetworkResolutionError& err) {
            future->SetFail(THttpFuture::CantResolveNameError, err.AsStrBuf());
        } catch (const yexception& err) {
            future->SetFail(THttpFuture::OtherError, err.AsStrBuf());
        }
    }

    bool THttpFuture::HasHttpCode() const {
        return !!HttpCode;
    }

    bool THttpFuture::HasResponseBody() const {
        return !!Response;
    }

    ui32 THttpFuture::GetHttpCode() const {
        Y_ASSERT(IsDone());
        Y_ASSERT(HasHttpCode());

        return static_cast<ui32>(*HttpCode);
    }

    TString THttpFuture::GetResponseBody() const {
        Y_ASSERT(IsDone());
        Y_ASSERT(HasResponseBody());

        return Response->Data;
    }

    void THttpFuture::SetDoneAndSchedule(TAutoPtr<NNeh::TResponse> response) {
        if (!response->IsError()) {
            ErrorCode = THttpFuture::NoError;
            HttpCode = HttpCodes::HTTP_OK;
        } else {
            ErrorCode = THttpFuture::BadHttpCodeError;
            ErrorDescription = response->GetErrorText();

            HttpCode = TryGetHttpCodeFromErrorDescription(ErrorDescription);
        }
        Response.Reset(response);
        SetDone();
    }

    void THttpFuture::SetFail(THttpFuture::EError errorCode, const TStringBuf& errorDescription) {
        ErrorCode = errorCode;
        ErrorDescription = errorDescription;
        Response.Destroy();
        SetDone();
    }

}
