#include "yql_servlet.h"


namespace NYql {
namespace NHttp {

///////////////////////////////////////////////////////////////////////////////
// TResponse
///////////////////////////////////////////////////////////////////////////////
void TResponse::OutTo(IOutputStream& out) const {
    TVector<IOutputStream::TPart> parts;
    const size_t FIRST_LINE_PARTS = 3;
    const size_t HEADERS_PARTS = Headers.Count() * 4;
    const size_t CONTENT_PARTS = 5;
    parts.reserve(FIRST_LINE_PARTS + HEADERS_PARTS + CONTENT_PARTS);

    // first line
    parts.push_back(IOutputStream::TPart(TStringBuf("HTTP/1.1 ")));
    parts.push_back(IOutputStream::TPart(HttpCodeStrEx(Code)));
    parts.push_back(IOutputStream::TPart::CrLf());

    // headers
    for (THttpHeaders::TConstIterator i = Headers.Begin(); i != Headers.End(); ++i) {
        parts.push_back(IOutputStream::TPart(i->Name()));
        parts.push_back(IOutputStream::TPart(TStringBuf(": ")));
        parts.push_back(IOutputStream::TPart(i->Value()));
        parts.push_back(IOutputStream::TPart::CrLf());
    }

    char buf[50];

    if (!Body.Empty()) {
        TMemoryOutput mo(buf, sizeof(buf));
        mo << Body.Size();

        parts.push_back(IOutputStream::TPart(TStringBuf("Content-Length: ")));
        parts.push_back(IOutputStream::TPart(buf, mo.Buf() - buf));
        parts.push_back(IOutputStream::TPart::CrLf());
    }

    // body
    parts.push_back(IOutputStream::TPart::CrLf());

    if (!Body.Empty()) {
        parts.push_back(IOutputStream::TPart(Body.AsCharPtr(), Body.Size()));
    }

    out.Write(parts.data(), parts.size());
}


} // namspace NNttp
} // namspace NYql
