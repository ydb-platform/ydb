#pragma once

#include <util/generic/fwd.h>

namespace NKikimr {

namespace NIceDb {
    class TNiceDb;
}

namespace NReplication::NController {

class ISysParamLoader {
public:
    virtual ~ISysParamLoader() = default;
    virtual ui64 LoadInt() = 0;
    virtual TString LoadText() = 0;
    virtual TString LoadBinary() = 0;
};

class TSysParams {
    enum class ESysParam: ui32 {
        NextReplicationId = 1,
    };

public:
    TSysParams();
    void Reset();
    void Load(ESysParam type, ISysParamLoader* loader);
    void Load(ui32 type, ISysParamLoader* loader);

    ui64 AllocateReplicationId(NIceDb::TNiceDb& db);

private:
    ui64 NextReplicationId;

}; // TSysParams

} // NReplication::NController
} // NKikimr
