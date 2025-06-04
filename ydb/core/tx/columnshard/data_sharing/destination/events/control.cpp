#include "control.h"
#include <ydb/core/tx/columnshard/data_sharing/destination/session/destination.h>

namespace NKikimr::NOlap::NDataSharing::NEvents {

TEvProposeFromInitiator::TEvProposeFromInitiator(const TDestinationSession& session, const NOlap::IPathIdTranslator& pathIdTranslator) {
    *Record.MutableSession() = session.SerializeDataToProto(pathIdTranslator);
}

TEvConfirmFromInitiator::TEvConfirmFromInitiator(const TString& sessionId) {
    *Record.MutableSessionId() = sessionId;
}

}