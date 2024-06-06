#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NYql {

struct TAwsSignature {
public:
    TAwsSignature(const TString& method, const TString& url, const TString& contentType, const TString& payload, const TString& awsSigV4, const TString& userPwd, const TInstant& currentTime = TInstant::Now());

    TString GetAuthorization() const;

    TString GetXAmzContentSha256() const;

    TString GetAmzDate() const;

    TString GetContentType() const;
    
private:
    TString GetListSignedHeaders() const;

    TString SignHeaders() const;

    TString CreateHttpCanonicalRequest() const;

    TString CreateStringToSign() const;

    TString CalcSignature() const;

    TString GetDate() const;

    static TString HmacSHA256(TStringBuf key, TStringBuf data);

    static TString HashSHA256(TStringBuf data);

    static TString UriEncode(const TStringBuf input, bool encodeSlash = false);

    void PrepareCgiParameters();

private:
    TString Host;
    TString Uri;
    TString Cgi;
    TString Method;
    TString AccessKey;
    TString AccessSecret;
    TString Region;
    TString Service;
    TString Url;
    TString ContentType;
    TString Payload;
    TInstant CurrentTime;
};

}
