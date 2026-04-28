#include "ic_storage_transport_events.h"

namespace NYdb::NBS::NBlockStore::NStorage::NTransport {

////////////////////////////////////////////////////////////////////////////////

TEvTransportPrivate::TConnect::~TConnect()
{
    Y_ABORT_UNLESS(Promise.IsReady());
}

TEvTransportPrivate::TWriteToPBuffer::~TWriteToPBuffer()
{
    Y_ABORT_UNLESS(Promise.IsReady());
}

TEvTransportPrivate::TWriteToManyPBuffers::~TWriteToManyPBuffers()
{
    Y_ABORT_UNLESS(Promise.IsReady());
}

TEvTransportPrivate::TWriteToDDisk::~TWriteToDDisk()
{
    Y_ABORT_UNLESS(Promise.IsReady());
}

TEvTransportPrivate::TEraseFromPBuffer::~TEraseFromPBuffer()
{
    Y_ABORT_UNLESS(Promise.IsReady());
}

TEvTransportPrivate::TReadFromPBuffer::~TReadFromPBuffer()
{
    Y_ABORT_UNLESS(Promise.IsReady());
}

TEvTransportPrivate::TReadFromDDisk::~TReadFromDDisk()
{
    Y_ABORT_UNLESS(Promise.IsReady());
}

TEvTransportPrivate::TSyncWithPBuffer::~TSyncWithPBuffer()
{
    Y_ABORT_UNLESS(Promise.IsReady());
}

TEvTransportPrivate::TListPBufferEntries::~TListPBufferEntries()
{
    Y_ABORT_UNLESS(Promise.IsReady());
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport
