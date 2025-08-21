#include "dq_channel_spiller_adapter.h"

namespace NYql::NDq {

IDqChannelStorage::TPtr CreateDqChannelSpillerAdapter(IDqSpiller::TPtr spiller) {
    return new TDqChannelSpillerAdapter(std::move(spiller));
}

} // namespace NYql::NDq
