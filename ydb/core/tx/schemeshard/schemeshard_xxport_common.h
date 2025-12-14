#pragma once

#include <ydb/core/backup/common/encryption.h>

#include <util/generic/fwd.h>

namespace NKikimr::NBackup {
    enum class EBackupFileType : unsigned char;
}

namespace NKikimrSchemeOp {
    enum EPathType : int;
}

namespace NKikimr::NSchemeShard {

struct XxportProperties {
    TString FileName;
    NBackup::EBackupFileType FileType;
};

const TVector<XxportProperties>& GetXxportProperties();

TMaybe<XxportProperties> PathTypeToXxportProperties(NKikimrSchemeOp::EPathType pathType);

}
