#include "policy.h"
#include <util/folder/path.h>
#include <ydb/core/sys_view/common/path.h>

namespace NKikimr::NOlap::NReader::NSysView::NAbstract {

THolder<ISysViewPolicy> ISysViewPolicy::BuildByPath(const TString& tablePath) {
    auto fixed = TFsPath(tablePath).Fix();
    if (fixed.Parent().GetName() != ::NKikimr::NSysView::SysPathName) {
        return nullptr;
    } else {
        return TFactory::MakeHolder(fixed.GetName());
    }
}

}