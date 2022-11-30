#include "data_ptr.h"

namespace NXdeltaAggregateColumn {

    TDeleter::TDeleter(XDeltaContext* context)
        : Context(context)
    {
    }

    void TDeleter::operator()(ui8* ptr) const
    {
        if (!Context) {
            free(ptr);
            return;
        }
        Context->free(Context->opaque, ptr);
    }
}
