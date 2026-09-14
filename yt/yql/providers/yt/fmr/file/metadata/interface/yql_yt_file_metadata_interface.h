#pragma once

#include <library/cpp/threading/future/future.h>

namespace NYql::NFmr {

class IFileMetadataService: public TThrRefBase {

public:
    virtual ~IFileMetadataService() = default;

    using TPtr = TIntrusivePtr<IFileMetadataService>;

    virtual NThreading::TFuture<bool> GetFileUploadStatus(const TString& md5key) = 0;
};

} // namespace NYql::NFmr
