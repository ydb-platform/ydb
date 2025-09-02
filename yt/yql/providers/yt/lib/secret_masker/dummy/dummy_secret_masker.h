#pragma once

#include <yt/yql/providers/yt/lib/secret_masker/secret_masker.h>

namespace NYql {

ISecretMasker::TPtr CreateDummySecretMasker();

} // namespace NYql
