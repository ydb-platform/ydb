#pragma once

#include <google/protobuf/any.pb.h>

#include <util/generic/maybe.h>

namespace NPq {
    struct TSourceWatermarksSettings {
        TMaybe<ui64> IdleTimeoutUs;
    };

    TMaybe<TSourceWatermarksSettings> GetSourceWatermarkSettings(const TString& provider, const google::protobuf::Any& settings);

} // namespace NPq
