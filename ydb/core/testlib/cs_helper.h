#pragma once
#include "common_helper.h"

namespace NKikimr::Tests::NCS {

class THelper: public NCommon::THelper {
private:
    using TBase = NCommon::THelper;
public:
    using TBase::TBase;
    void CreateTestOlapStore(TActorId sender, TString scheme);
    void CreateTestOlapTable(TActorId sender, TString storeName, TString scheme);
};

}
