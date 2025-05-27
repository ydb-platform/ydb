#include "download_stream.h"

namespace NYql {

TDownloadStream::TDownloadStream(IInputStream& delegatee)
    : Delegatee_(delegatee)
{
}

size_t TDownloadStream::DoRead(void* buf, size_t len) {
    try {
        return Delegatee_.Read(buf, len);
    } catch (const std::exception& e) {
        // just change type of the exception
        throw TDownloadError() << e.what();
    }
}

}
