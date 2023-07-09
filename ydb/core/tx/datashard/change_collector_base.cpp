#include "change_collector_base.h"
#include "datashard_impl.h"
#include "datashard_user_db.h"

#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

namespace NKikimr {
namespace NDataShard {

using namespace NMiniKQL;
using namespace NTable;

TBaseChangeCollector::TBaseChangeCollector(TDataShard* self, IDataShardUserDb& userDb, IBaseChangeCollectorSink& sink)
    : Self(self)
    , UserDb(userDb)
    , Sink(sink)
{
}

void TBaseChangeCollector::OnRestart() {
    // nothing
}

bool TBaseChangeCollector::NeedToReadKeys() const {
    return false;
}

} // NDataShard
} // NKikimr
