#include "object.h"
#include "behaviour.h"

namespace NKikimr::NKqp {

NMetadata::IClassBehaviour::TPtr TStreamingQueryConfig::GetBehaviour() {
    return TStreamingQueryBehaviour::GetInstance();
}

TString TStreamingQueryConfig::GetTypeId() {
    return "STREAMING_QUERY";
}

}  // namespace NKikimr::NKqp
