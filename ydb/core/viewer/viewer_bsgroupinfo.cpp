#include "viewer_bsgroupinfo.h"

namespace NKikimr::NViewer {

const TWhiteboardMergerBase::TRegistrator TWhiteboardInfo<NKikimrWhiteboard::TEvBSGroupStateResponse>::Registrator({
    {NKikimrWhiteboard::TBSGroupStateInfo::descriptor()->FindFieldByName("Latency"), &TWhiteboardMergerBase::ProtoMaximizeEnumField}
});

} // namespace NKikimr::NViewer
