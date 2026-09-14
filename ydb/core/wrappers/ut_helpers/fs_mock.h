#pragma once

#include "backup_mock.h"

#include <util/generic/hash.h>
#include <util/generic/string.h>

namespace NKikimr::NWrappers::NTestHelpers {

class TFsMock : public TBackupMock {
public:
    explicit TFsMock(const TString& basePath);

    void Refresh();

    const THashMap<TString, TString>& GetData() const override;
    THashMap<TString, TString>& GetData() override;

    const TString& GetBasePath() const;

private:
    TString BasePath_;
    THashMap<TString, TString> Data_;
};

} // NKikimr::NWrappers::NTestHelpers
