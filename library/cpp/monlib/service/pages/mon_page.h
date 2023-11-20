#pragma once

#include <library/cpp/monlib/service/service.h>
#include <library/cpp/monlib/service/mon_service_http_request.h>

#include <util/generic/string.h>
#include <util/generic/ptr.h>

namespace NMonitoring {
    static const char HTTPOKTEXT[] = "HTTP/1.1 200 Ok\r\nContent-Type: text/plain\r\nConnection: Close\r\n\r\n";
    static const char HTTPOKBIN[] = "HTTP/1.1 200 Ok\r\nContent-Type: application/octet-stream\r\nConnection: Close\r\n\r\n";
    static const char HTTPOKHTML[] = "HTTP/1.1 200 Ok\r\nContent-Type: text/html\r\nConnection: Close\r\n\r\n";
    static const char HTTPOKJSON[] = "HTTP/1.1 200 Ok\r\nContent-Type: application/json\r\nConnection: Close\r\n\r\n";
    static const char HTTPOKSPACK[] = "HTTP/1.1 200 Ok\r\nContent-Type: application/x-solomon-spack\r\nConnection: Close\r\n\r\n";
    static const char HTTPOKPROMETHEUS[] = "HTTP/1.1 200 Ok\r\nContent-Type: text/plain\r\nConnection: Close\r\n\r\n";
    static const char HTTPOKUNISTAT[] = "HTTP/1.1 200 Ok\r\nContent-Type: text/json\r\nConnection: Close\r\n\r\n";
    static const char HTTPOKJAVASCRIPT[] = "HTTP/1.1 200 Ok\r\nContent-Type: text/javascript\r\nConnection: Close\r\n\r\n";
    static const char HTTPOKCSS[] = "HTTP/1.1 200 Ok\r\nContent-Type: text/css\r\nConnection: Close\r\n\r\n";
    static const char HTTPNOCONTENT[] = "HTTP/1.1 204 No content\r\nConnection: Close\r\n\r\n";
    static const char HTTPNOTFOUND[] = "HTTP/1.1 404 Invalid URI\r\nConnection: Close\r\n\r\nInvalid URL\r\n";
    static const char HTTPUNAUTHORIZED[] = "HTTP/1.1 401 Unauthorized\r\nConnection: Close\r\n\r\nUnauthorized\r\n";
    static const char HTTPFORBIDDEN[] = "HTTP/1.1 403 Forbidden\r\nConnection: Close\r\n\r\nForbidden\r\n";
    static const char HTTPOKJAVASCRIPT_CACHED[] = "HTTP/1.1 200 Ok\r\nContent-Type: text/javascript\r\nCache-Control: public, max-age=31536000\r\nConnection: Close\r\n\r\n";
    static const char HTTPOKCSS_CACHED[] = "HTTP/1.1 200 Ok\r\nContent-Type: text/css\r\nCache-Control: public, max-age=31536000\r\nConnection: Close\r\n\r\n";

    // Fonts
    static const char HTTPOKFONTEOT[] = "HTTP/1.1 200 Ok\r\nContent-Type: application/vnd.ms-fontobject\r\nConnection: Close\r\n\r\n";
    static const char HTTPOKFONTTTF[] = "HTTP/1.1 200 Ok\r\nContent-Type: application/x-font-ttf\r\nConnection: Close\r\n\r\n";
    static const char HTTPOKFONTWOFF[] = "HTTP/1.1 200 Ok\r\nContent-Type: application/font-woff\r\nConnection: Close\r\n\r\n";
    static const char HTTPOKFONTWOFF2[] = "HTTP/1.1 200 Ok\r\nContent-Type: application/font-woff2\r\nConnection: Close\r\n\r\n";
    static const char HTTPOKFONTEOT_CACHED[] = "HTTP/1.1 200 Ok\r\nContent-Type: application/vnd.ms-fontobject\r\nCache-Control: public, max-age=31536000\r\nConnection: Close\r\n\r\n";
    static const char HTTPOKFONTTTF_CACHED[] = "HTTP/1.1 200 Ok\r\nContent-Type: application/x-font-ttf\r\nCache-Control: public, max-age=31536000\r\nConnection: Close\r\n\r\n";
    static const char HTTPOKFONTWOFF_CACHED[] = "HTTP/1.1 200 Ok\r\nContent-Type: application/font-woff\r\nCache-Control: public, max-age=31536000\r\nConnection: Close\r\n\r\n";
    static const char HTTPOKFONTWOFF2_CACHED[] = "HTTP/1.1 200 Ok\r\nContent-Type: application/font-woff2\r\nCache-Control: public, max-age=31536000\r\nConnection: Close\r\n\r\n";

    // Images
    static const char HTTPOKPNG[] = "HTTP/1.1 200 Ok\r\nContent-Type: image/png\r\nConnection: Close\r\n\r\n";
    static const char HTTPOKSVG[] = "HTTP/1.1 200 Ok\r\nContent-Type: image/svg+xml\r\nConnection: Close\r\n\r\n";

    class IMonPage;

    using TMonPagePtr = TIntrusivePtr<IMonPage>;

    class IMonPage: public TAtomicRefCount<IMonPage> {
    public:
        const TString Path;
        const TString Title;
        const IMonPage* Parent = nullptr;

    public:
        IMonPage(const TString& path, const TString& title = TString());

        virtual ~IMonPage() {
        }

        void OutputNavBar(IOutputStream& out);

        virtual const TString& GetPath() const {
            return Path;
        }

        virtual const TString& GetTitle() const {
            return Title;
        }

        bool IsInIndex() const {
            return !Title.empty();
        }

        virtual bool IsIndex() const {
            return false;
        }

        virtual void Output(IMonHttpRequest& request) = 0;
    };

}
