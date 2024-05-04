#include "policy.h"
#include <util/folder/path.h>

namespace NKikimr::NOlap::NReader::NSysView::NAbstract {

THolder<ISysViewPolicy> ISysViewPolicy::BuildByPath(const TString& tablePath) {
    auto fixed = TFsPath(tablePath).Fix();
    if (fixed.Parent().GetName() != ".sys") {
        return nullptr;
    } else {
        return TFactory::MakeHolder(fixed.GetName());
    }
}

}