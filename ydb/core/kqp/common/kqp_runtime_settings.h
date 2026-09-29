#pragma once

#include <yql/essentials/minikql/runtime_settings/proto/runtime_settings.pb.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings.h>

namespace NKikimr::NKqp {

class TKqpRuntimeSettings {
public:
    TKqpRuntimeSettings();

    const NYql::TRuntimeSettings::TConstPtr& Get() const;
    void ApplyTo(NYql::NProto::TRuntimeSettings& proto) const;

private:
    NYql::TRuntimeSettings::TConstPtr Settings_;
};

} // namespace NKikimr::NKqp
