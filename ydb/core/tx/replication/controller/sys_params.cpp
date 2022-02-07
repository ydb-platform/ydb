#include "schema.h"
#include "sys_params.h"

namespace NKikimr {
namespace NReplication {
namespace NController {

TSysParams::TSysParams() {
    Reset();
}

void TSysParams::Reset() {
    NextReplicationId = 1;
}

void TSysParams::Load(ESysParam type, ISysParamLoader* loader) {
    switch (type) {
    case ESysParam::NextReplicationId:
        NextReplicationId = loader->LoadInt();
        break;
    default:
        break; // ignore
    }
}

void TSysParams::Load(ui32 type, ISysParamLoader* loader) {
    Load(ESysParam(type), loader);
}

ui64 TSysParams::AllocateReplicationId(NIceDb::TNiceDb& db) {
    using Schema = TControllerSchema;
    const auto result = NextReplicationId++;

    db.Table<Schema::SysParams>().Key(ui32(ESysParam::NextReplicationId)).Update(
        NIceDb::TUpdate<Schema::SysParams::IntValue>(NextReplicationId)
    );

    return result;
}

} // NController
} // NReplication
} // NKikimr
