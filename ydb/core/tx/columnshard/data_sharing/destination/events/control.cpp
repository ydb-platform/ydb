#include "control.h"
#include <ydb/core/tx/columnshard/data_sharing/destination/session/destination.h>

namespace NKikimr::NOlap::NDataSharing::NEvents {

TEvProposeFromInitiator::TEvProposeFromInitiator(const TDestinationSession& session) {
    *Record.MutableSession() = session.SerializeDataToProto();
}

TEvConfirmFromInitiator::TEvConfirmFromInitiator(const TString& sessionId) {
    *Record.MutableSessionId() = sessionId;
}

}