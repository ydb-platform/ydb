#pragma once

#include <util/generic/string.h>

namespace NKikimr::NBackup {

class IChecksum {
public:
    using TPtr = std::unique_ptr<IChecksum>;

    virtual ~IChecksum() = default;

    virtual void AddData(TStringBuf data) = 0;
    virtual TString Serialize() = 0;
};

IChecksum* CreateChecksum();
TString ComputeChecksum(TStringBuf data);
TString ChecksumKey(const TString& objKey);

} // NKikimr::NBackup
