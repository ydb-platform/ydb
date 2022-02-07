#include "types.h"

#include <util/generic/is_in.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

namespace NKikimr::NSQS {

extern const TString XML_CONTENT_TYPE = "application/xml";
extern const TString PLAIN_TEXT_CONTENT_TYPE = "text/plain";

TSqsHttpResponse::TSqsHttpResponse(const TString& body, int status, const TString& contentType)
    : Body(body)
    , ContentType(contentType)
    , StatusCode(status)
{
}

} // namespace NKikimr::NSQS
