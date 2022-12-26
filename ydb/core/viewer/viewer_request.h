#pragma once

#include "viewer.h"

namespace NKikimr {
namespace NViewer {

using namespace NActors;


IActor* CreateViewerRequestHandler(TEvViewer::TEvViewerRequest::TPtr request);

}
}
