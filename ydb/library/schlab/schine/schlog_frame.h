#pragma once
#include "defs.h"

namespace NKikimr {
namespace NSchLab {

enum EFrameType {
  FrameTypeKey = 1,
  FrameTypeAddCbs,
  FrameTypeAddJob,
  FrameTypeSelectJob,
  FrameTypeCompleteJob,
  FrameTypeRemoveJob
};

} // NSchLab
} // NKikimr
