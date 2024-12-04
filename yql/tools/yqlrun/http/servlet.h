#pragma once

#include <library/cpp/http/misc/httpcodes.h>
#include <library/cpp/http/misc/httpreqdata.h>
#include <library/cpp/http/io/stream.h>

#include <util/memory/blob.h>


namespace NYql {
namespace NHttp {

///////////////////////////////////////////////////////////////////////////////
// THttpError
///////////////////////////////////////////////////////////////////////////////
class THttpError: public yexception
{
public:
    inline THttpError(HttpCodes code)
        : Code_(code)
    {
    }

    inline HttpCodes GetCode() const {
        return Code_;
    }

private:
    HttpCodes Code_;
};

///////////////////////////////////////////////////////////////////////////////
// TRequest & TResponse
///////////////////////////////////////////////////////////////////////////////
struct TRequest
{
    const THttpInput& Input;
    const TServerRequestData& RD;
    const TBlob& Body;
};

struct TResponse
{
    HttpCodes Code;
    THttpHeaders Headers;
    TBlob Body;
    TString ContentType;

    inline TResponse()
        : Code(HTTP_OK)
        , ContentType(TStringBuf("text/plain"))
    {
    }

    void OutTo(IOutputStream& out) const;
};

///////////////////////////////////////////////////////////////////////////////
// IServlet
///////////////////////////////////////////////////////////////////////////////
class IServlet: private TNonCopyable
{
public:
    virtual ~IServlet() = default;

    virtual void DoGet(const TRequest&, TResponse& resp) const {
        resp.Code = HttpCodes::HTTP_NOT_IMPLEMENTED;
    }

    virtual void DoPost(const TRequest&, TResponse& resp) const {
        resp.Code = HttpCodes::HTTP_NOT_IMPLEMENTED;
    }
};

} // namspace NNttp
} // namspace NYql

