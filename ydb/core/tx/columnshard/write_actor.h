#pragma once

#include <ydb/core/tx/columnshard/engines/writer/write_controller.h>

#include <util/datetime/base.h>

namespace NKikimr::NColumnShard {

NActors::IActor* CreateWriteActor(ui64 tabletId, IWriteController::TPtr writeController, const TInstant deadline);

}
