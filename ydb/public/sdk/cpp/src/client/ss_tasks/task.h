#pragma once

#include <ydb-cpp-sdk/client/driver/driver.h>
#include <ydb-cpp-sdk/client/types/operation/operation.h>

#include <ydb-cpp-sdk/client/types/s3_settings.h>

namespace NYdb::inline V3 {
namespace NSchemeShard {

class TBackgroundProcessesResponse: public TOperation {
private:
    using TBase = TOperation;
public:
    struct TMetadata {
        std::string Id;
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
