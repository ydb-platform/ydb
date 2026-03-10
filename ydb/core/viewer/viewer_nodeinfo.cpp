#include "viewer_nodeinfo.h"

namespace NKikimr::NViewer {

const TWhiteboardMergerBase::TRegistrator TWhiteboardInfo<NKikimrWhiteboard::TEvNodeStateResponse>::Registrator({
    {NKikimrWhiteboard::TNodeStateInfo::descriptor()->FindFieldByName("ConnectStatus"), &TWhiteboardMergerBase::ProtoMaximizeEnumField},
    {NKikimrWhiteboard::TNodeStateInfo::descriptor()->FindFieldByName("Connected"), &TWhiteboardMergerBase::ProtoMaximizeBoolField}
});

} // namespace NKikimr::NViewer
