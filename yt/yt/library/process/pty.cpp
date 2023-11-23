#include "pty.h"

#include "io_dispatcher.h"

#include <yt/yt/core/misc/common.h>
#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/net/connection.h>

namespace NYT::NPipes {

using namespace NNet;

////////////////////////////////////////////////////////////////////////////////

TPty::TPty(int height, int width)
{
    SafeOpenPty(&MasterFD_, &SlaveFD_, height, width);
}

TPty::~TPty()
{
    if (MasterFD_ != InvalidFD) {
        YT_VERIFY(TryClose(MasterFD_, false));
    }

    if (SlaveFD_ != InvalidFD) {
        YT_VERIFY(TryClose(SlaveFD_, false));
    }
}

IConnectionWriterPtr TPty::CreateMasterAsyncWriter()
{
    YT_VERIFY(MasterFD_ != InvalidFD);
    int fd = SafeDup(MasterFD_);
    SafeSetCloexec(fd);
    SafeMakeNonblocking(fd);
    return CreateConnectionFromFD(fd, {}, {}, TIODispatcher::Get()->GetPoller());
}

IConnectionReaderPtr TPty::CreateMasterAsyncReader()
{
    YT_VERIFY(MasterFD_ != InvalidFD);
    int fd = SafeDup(MasterFD_);
    SafeSetCloexec(fd);
    SafeMakeNonblocking(fd);
    return CreateConnectionFromFD(fd, {}, {}, TIODispatcher::Get()->GetPoller());
}

int TPty::GetMasterFD() const
{
    YT_VERIFY(MasterFD_ != InvalidFD);
    return MasterFD_;
}

int TPty::GetSlaveFD() const
{
    YT_VERIFY(SlaveFD_ != InvalidFD);
    return SlaveFD_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPipes
