#pragma once

#include <yql/essentials/core/qplayer/storage/interface/yql_qstorage.h>
#include <yql/essentials/providers/common/gateway/yql_provider_gateway.h>

#include <util/generic/ptr.h>

namespace NYql {

class IYtFullCapture : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IYtFullCapture>;

    virtual void ReportError(const std::exception& e) = 0;
    virtual void AddOperationFuture(const NThreading::TFuture<NCommon::TOperationResult>& future) = 0;
    virtual bool Seal() = 0;
    virtual bool IsReady() const = 0;
};

IYtFullCapture::TPtr CreateYtFullCapture();

} // namespace NYql
