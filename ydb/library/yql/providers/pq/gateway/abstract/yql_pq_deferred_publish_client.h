#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/deferred_publications.h>

#include <util/generic/fwd.h>
#include <util/generic/ptr.h>

namespace NYql {

class IDeferredPublishClient : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IDeferredPublishClient>;

    virtual NYdb::NTopic::TAsyncBeginPublicationResult BeginPublication(const TString& extPublicationId, const NYdb::NTopic::TBeginPublicationSettings& settings = {}) = 0;

    virtual NYdb::NTopic::TAsyncPublishResult Publish(const NYdb::NTopic::TDeferredPublication& publication, const NYdb::NTopic::TPublishSettings& settings = {}) = 0;
};

} // namespace NYql
