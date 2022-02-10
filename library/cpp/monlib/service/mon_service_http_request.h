#pragma once

#include "service.h"

#include <util/stream/output.h>

namespace NMonitoring {
    class TMonService2;
    class IMonPage;

    // XXX: IHttpRequest is already taken
    struct IMonHttpRequest {
        virtual ~IMonHttpRequest();

        virtual IOutputStream& Output() = 0;

        virtual HTTP_METHOD GetMethod() const = 0;
        virtual TStringBuf GetPath() const = 0;
        virtual TStringBuf GetPathInfo() const = 0;
        virtual TStringBuf GetUri() const = 0;
        virtual const TCgiParameters& GetParams() const = 0;
        virtual const TCgiParameters& GetPostParams() const = 0;
        virtual TStringBuf GetPostContent() const = 0;
        virtual const THttpHeaders& GetHeaders() const = 0;
        virtual TStringBuf GetHeader(TStringBuf name) const = 0;
        virtual TStringBuf GetCookie(TStringBuf name) const = 0;
        virtual TString GetRemoteAddr() const = 0;

        virtual TString GetServiceTitle() const = 0;

        virtual IMonPage* GetPage() const = 0;

        virtual IMonHttpRequest* MakeChild(IMonPage* page, const TString& pathInfo) const = 0;
    };

    struct TMonService2HttpRequest: IMonHttpRequest {
        IOutputStream* const Out;
        const IHttpRequest* const HttpRequest;
        TMonService2* const MonService;
        IMonPage* const MonPage;
        const TString PathInfo;
        TMonService2HttpRequest* const Parent;

        TMonService2HttpRequest(
            IOutputStream* out, const IHttpRequest* httpRequest,
            TMonService2* monService, IMonPage* monPage,
            const TString& pathInfo,
            TMonService2HttpRequest* parent)
            : Out(out)
            , HttpRequest(httpRequest)
            , MonService(monService)
            , MonPage(monPage)
            , PathInfo(pathInfo)
            , Parent(parent)
        {
        }

        ~TMonService2HttpRequest() override;

        IOutputStream& Output() override;
        HTTP_METHOD GetMethod() const override;
        TStringBuf GetPath() const override;
        TStringBuf GetPathInfo() const override;
        TStringBuf GetUri() const override;
        const TCgiParameters& GetParams() const override;
        const TCgiParameters& GetPostParams() const override;
        TStringBuf GetPostContent() const override {
            return HttpRequest->GetPostContent();
        }

        TStringBuf GetHeader(TStringBuf name) const override;
        TStringBuf GetCookie(TStringBuf name) const override;
        const THttpHeaders& GetHeaders() const override;
        TString GetRemoteAddr() const override;

        IMonPage* GetPage() const override {
            return MonPage;
        }

        TMonService2HttpRequest* MakeChild(IMonPage* page, const TString& pathInfo) const override {
            return new TMonService2HttpRequest{
                Out, HttpRequest, MonService, page,
                pathInfo, const_cast<TMonService2HttpRequest*>(this)
            };
        }

        TString GetServiceTitle() const override;
    };

}
