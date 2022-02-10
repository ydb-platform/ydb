#pragma once

#include <ydb/core/protos/node_whiteboard.pb.h>

namespace NKikimr {
namespace NCms {

using TPDiskStateInfo = NKikimrWhiteboard::TPDiskStateInfo;
using EPDiskState = NKikimrBlobStorage::TPDiskState::E;

} // NCms
} // NKikimr
