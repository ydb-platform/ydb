#include "table_writer_impl.h"

Y_DECLARE_OUT_SPEC(inline, NKikimr::NBackup::NImpl::TChangeRecord, out, value) {
    return value.Out(out);
}

Y_DECLARE_OUT_SPEC(inline, NKikimr::NBackup::NImpl::TChangeRecord::TPtr, out, value) {
    return value->Out(out);
}

namespace NKikimr::NBackup::NImpl {

IActor* CreateLocalTableWriter(const TPathId& tablePathId, EWriterType type) {
    return new NReplication::NService::TLocalTableWriter<NBackup::NImpl::TChangeRecord>(tablePathId, type);
}

}
