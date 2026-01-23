#include "watermark_settings.h"

#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
namespace NPq {
    TMaybe<TSourceWatermarksSettings> GetSourceWatermarkSettings(const TString& sourceType, const google::protobuf::Any& settings) {
        NYql::NPq::NProto::TDqPqTopicSource pqSettings;
        if (sourceType != "PqSource" || !settings.Is<decltype(pqSettings)>() || !settings.UnpackTo(&pqSettings)) {
            return Nothing();
        }
        if (!pqSettings.HasWatermarks()) {
            return Nothing();
        }
        TSourceWatermarksSettings watermarksSettings;
        const auto& watermarks = pqSettings.GetWatermarks();
        if (watermarks.HasIdleTimeoutUs()) {
            watermarksSettings.IdleTimeoutUs = watermarks.GetIdleTimeoutUs();
        }
        return watermarksSettings;
    }
} // namespace NPq
