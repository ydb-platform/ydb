#include "datastreams_serialization.h"

namespace NKikimr::NHttpProxy::NDataStreams {

    template<>
    void PrepareValue<Ydb::DataStreams::V1::ListStreamsRequest>(Ydb::DataStreams::V1::ListStreamsRequest& value) {
        value.set_recurse(true);
    }

} // namespace NKikimr::NHttpProxy::NDataStreams