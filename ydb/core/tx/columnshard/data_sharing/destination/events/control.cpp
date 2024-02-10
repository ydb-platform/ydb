#include "control.h"
#include <ydb/core/tx/columnshard/data_sharing/destination/session/destination.h>

namespace NKikimr::NOlap::NDataSharing::NEvents {

TEvStartFromInitiator::TEvStartFromInitiator(const TDestinationSession& session) {
    *Record.MutableSession() = session.SerializeDataToProto();
}

}