#pragma once

namespace Aws::S3 {
    class S3Error;
}

namespace NKikimr::NWrappers {

bool ShouldRetry(const Aws::S3::S3Error& error);
bool ShouldBackoff(const Aws::S3::S3Error& error);

}
