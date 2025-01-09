#pragma once

#include <library/cpp/scheme/scheme.h>
#include <ydb/core/ymq/base/action.h>

#include <util/generic/string.h>

namespace NKikimr::NSQS {

extern const TString XML_CONTENT_TYPE;
extern const TString PLAIN_TEXT_CONTENT_TYPE;

struct TSqsHttpResponse {
    TString Body;
    TString ContentType;
    int     StatusCode = 0;

    TString FolderId;
    TString ResourceId;
    bool    IsFifo = false;

    NSc::TValue QueueTags;

    TSqsHttpResponse() = default;
    TSqsHttpResponse(const TString& body, int status, const TString& contentType = XML_CONTENT_TYPE);
};

} // namespace NKikimr::NSQS
