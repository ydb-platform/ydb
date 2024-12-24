#pragma once

#include <openssl/sha.h>

#include <util/generic/string.h>

namespace NKikimr::NDataShard {

class IExportChecksum {
public:
    using TPtr = std::unique_ptr<IExportChecksum>;

    virtual ~IExportChecksum() = default;

    virtual void AddData(TStringBuf data) = 0;
    virtual TString Serialize() = 0;

    static TString Compute(TStringBuf data);
};

IExportChecksum* CreateExportChecksum();

} // NKikimr::NDataShard
