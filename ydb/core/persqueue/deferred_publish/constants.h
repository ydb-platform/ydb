#pragma once

#include <util/generic/string.h>
#include <util/string/builder.h>

namespace NKikimr::NPQ::NDeferredPublish {

inline constexpr TStringBuf DisabledMessage = "Topic deferred publish is not enabled";
inline constexpr size_t MaxDeferredPublishStringLength = 2048;

inline TString MakeDeferredPublishWriterKey(ui64 intPublicationId) {
    return TStringBuilder() << "defpubsvc:" << intPublicationId;
}

inline bool IsDeferredPublishWriterKey(TStringBuf txId) {
    return txId.StartsWith("defpubsvc:");
}

} // namespace NKikimr::NPQ::NDeferredPublish
