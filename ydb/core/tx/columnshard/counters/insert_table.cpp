#include "insert_table.h"

namespace NKikimr::NColumnShard {

TInsertTableCounters::TInsertTableCounters()
    : TBase("InsertTable")
    , PathIdInserted("InsertTable", "Inserted")
    , PathIdCommitted("InsertTable", "Committed")
    , Inserted("InsertTable", "Inserted")
    , Committed("InsertTable", "Committed")
    , Aborted("InsertTable", "Aborted")
{
}

}
