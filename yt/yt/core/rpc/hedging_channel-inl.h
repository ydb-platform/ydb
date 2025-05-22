#ifndef HEDGING_CHANNEL_INL_H_
#error "Direct inclusion of this file is not allowed, include hedging_channel.h"
// For the sake of sane code completion.
#include "hedging_channel.h"
#endif
#undef HEDGING_CHANNEL_INL_H_

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

static const TString BackupFailedKey("backup_failed");

template <class T>
bool IsBackup(const TErrorOr<TIntrusivePtr<T>>& responseOrError)
{
    return responseOrError.IsOK()
        ? IsBackup(responseOrError.Value())
        : responseOrError.Attributes().template Get<bool>(BackupFailedKey, false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
