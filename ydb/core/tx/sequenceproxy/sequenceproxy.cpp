#include "sequenceproxy.h"
#include "sequenceproxy_impl.h"

namespace NKikimr {
namespace NSequenceProxy {

    IActor* CreateSequenceProxy(const TSequenceProxySettings& settings) {
        return new TSequenceProxy(settings);
    }

} // namespace NSequenceProxy
} // namespace NKikimr
