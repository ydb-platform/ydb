#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TEtag
{
    NObjectClient::TObjectId Id = NObjectClient::NullObjectId;
    NHydra::TRevision Revision = NHydra::NullRevision;
};

bool operator==(const TEtag& lhs, const TEtag& rhs);

TErrorOr<TEtag> ParseEtag(TStringBuf etagString);
void FormatValue(TStringBuilderBase* builder, const TEtag& tag, TStringBuf spec);

void WriteFileByBatches(const NApi::IFileWriterPtr& writer, const TSharedRef& data, i64 maxAttachmentSize);

////////////////////////////////////////////////////////////////////////////////

void PutMethodInfoInTraceContext(TStringBuf methodName);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
