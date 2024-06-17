#include "json_change_record.h"
#include "table_writer_impl.h"

namespace NKikimr::NReplication::NService {

IActor* CreateLocalTableWriter(const TPathId& tablePathId) {
    return new TLocalTableWriter<NReplication::NService::TChangeRecord>(tablePathId);
}

}
