#pragma once

#include <library/cpp/threading/future/future.h>

namespace NYql::NFmr {

class IFileUploadService: public TThrRefBase {

public:
    virtual ~IFileUploadService() = default;

    using TPtr = TIntrusivePtr<IFileUploadService>;

    virtual NThreading::TFuture<void> UploadObject(const TString& md5Key, const TString& fileName) = 0;
};

} // namespace NYql::NFmr
