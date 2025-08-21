#pragma once

#include <ydb/core/base/defs.h>

#include <util/generic/string.h>

namespace NKikimr {
    struct TPathId;
}

namespace NKikimrReplication {
    class TBatchingSettings;
}

namespace NKikimr::NReplication::NService {

class ITransferWriterFactory {
public:
    struct Parameters {
        const TString& TransformLambda;
        const TPathId& TablePathId;
        const TActorId& CompileServiceId;
        const NKikimrReplication::TBatchingSettings& BatchingSettings;
        const TString& RunAsUser;
        const TString& DirectoryPath;
        const TString& Database;
    };

    virtual IActor* Create(const Parameters& parameters) const = 0;

    virtual ~ITransferWriterFactory() = default;
};


}
