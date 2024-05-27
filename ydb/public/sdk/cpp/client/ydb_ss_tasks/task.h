#pragma once

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_types/operation/operation.h>

#include <ydb/public/sdk/cpp/client/ydb_types/s3_settings.h>

namespace NYdb {
namespace NSchemeShard {

class TBackgroundProcessesResponse: public TOperation {
private:
    using TBase = TOperation;
public:
    struct TMetadata {
        TString Id;
    };

public:
    using TOperation::TOperation;
    TBackgroundProcessesResponse(TStatus&& status, Ydb::Operations::Operation&& operation);

    const TMetadata& Metadata() const;

private:
    TMetadata Metadata_;
};

} // namespace NExport
} // namespace NYdb
