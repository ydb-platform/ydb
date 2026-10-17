#include "events_writer_iface.h"

namespace NKikimr::NSQS {

void IEventsWriterWrapper::Close() {
    if (!Closed) {
        Closed = true;
        CloseImpl();
    }
}

} // namespace NKikimr::NSQS
