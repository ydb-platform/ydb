#pragma once

#include <yt/yt/client/signature/public.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NFileClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TFileChunkWriterConfig)

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_STRONG_TYPEDEF(TSignedDistributedWriteFileSessionPtr, NSignature::TSignaturePtr);
YT_DEFINE_STRONG_TYPEDEF(TSignedWriteFileFragmentCookiePtr, NSignature::TSignaturePtr);
YT_DEFINE_STRONG_TYPEDEF(TSignedWriteFileFragmentResultPtr, NSignature::TSignaturePtr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFileClient
