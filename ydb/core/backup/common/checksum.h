#pragma once

#include <ydb/core/protos/checksum.pb.h>

#include <util/generic/string.h>

namespace NKikimr::NBackup {

using NKikimrBackup::TChecksumState;

class IChecksum {
public:
    using TPtr = std::unique_ptr<IChecksum>;

    virtual ~IChecksum() = default;

    virtual void AddData(TStringBuf data) = 0;
    virtual TString Finalize() = 0;

    virtual TChecksumState GetState() const = 0;
    virtual void Continue(const TChecksumState& state) = 0;
};

IChecksum* CreateChecksum();
TString ComputeChecksum(TStringBuf data);
TString ChecksumKey(const TString& objKey);

} // NKikimr::NBackup
