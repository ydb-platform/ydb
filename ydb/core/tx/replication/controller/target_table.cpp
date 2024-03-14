#include "target_table.h"

namespace NKikimr::NReplication::NController {

TTableTarget::TTableTarget(ui64 id, const TString& srcPath, const TString& dstPath)
    : TTargetWithStream(ETargetKind::Table, id, srcPath, dstPath)
{
}

}
