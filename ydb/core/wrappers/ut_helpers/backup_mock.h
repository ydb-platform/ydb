#pragma once

#include <util/generic/hash.h>
#include <util/generic/string.h>

namespace NKikimr::NWrappers::NTestHelpers {
class TBackupMock {
public:
    virtual ~TBackupMock() = default;

    virtual const THashMap<TString, TString>& GetData() const = 0;
    virtual THashMap<TString, TString>& GetData() = 0;
};

} // namespace NKikimr::NWrappers::NTestHelpers
