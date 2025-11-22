#include "session.h"

#include <ydb/core/tx/columnshard/bg_tasks/abstract/adapter.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/export/actor/export_actor.h>

namespace NKikimr::NOlap::NExport {

NKikimr::TConclusion<std::unique_ptr<NActors::IActor>> TSession::DoCreateActor(const NBackground::TStartContext& context) const {
    AFL_VERIFY(IsConfirmed());
    Status = EStatus::Started;
    return std::make_unique<TActor>(context.GetSessionSelfPtr(), context.GetAdapter());
}

}   // namespace NKikimr::NOlap::NExport
