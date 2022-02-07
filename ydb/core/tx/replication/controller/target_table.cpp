#include "target_table.h"

namespace NKikimr {
namespace NReplication {
namespace NController {

TTableTarget::TTableTarget(ui64 rid, ui64 tid, const TString& srcPath, const TString& dstPath)
    : TTargetWithStream(ETargetKind::Table, rid, tid, srcPath, dstPath)
{
}

} // NController
} // NReplication
} // NKikimr
