#pragma once

#include <ydb-cpp-sdk/client/driver/driver.h>
#include <ydb-cpp-sdk/client/scheme/scheme.h>

namespace Ydb::View {
    class DescribeViewResult;
}

namespace NYdb::inline V3 {
    class TProtoAccessor;
}

namespace NYdb::inline V3::NView {

class TDescribeViewResult;
using TAsyncDescribeViewResult = NThreading::TFuture<TDescribeViewResult>;

struct TDescribeViewSettings : public TOperationRequestSettings<TDescribeViewSettings> {
    using TSelf = TDescribeViewSettings;
};

class TViewDescription {
public:
    explicit TViewDescription(const Ydb::View::DescribeViewResult& desc);

    const std::string& GetQueryText() const;

private:
    std::string QueryText_;
};

class TDescribeViewResult : public NScheme::TDescribePathResult {
    friend class NYdb::V3::TProtoAccessor;
    const Ydb::View::DescribeViewResult& GetProto() const;

public:
    TDescribeViewResult(TStatus&& status, Ydb::View::DescribeViewResult&& desc);
    TViewDescription GetViewDescription() const;

private:
    std::unique_ptr<Ydb::View::DescribeViewResult> Proto_;
};

class TViewClient {
    class TImpl;

public:
    TViewClient(const TDriver& driver, const TCommonClientSettings& settings = TCommonClientSettings());

    TAsyncDescribeViewResult DescribeView(const std::string& path,
        const TDescribeViewSettings& settings = TDescribeViewSettings());

private:
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NYdb::V3::NView
