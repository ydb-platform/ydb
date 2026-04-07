#pragma once

#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TPartitionDatabase: public NKikimr::NIceDb::TNiceDb
{
public:
    enum class EBlobIndexScanProgress
    {
        NotReady,
        Completed,
        Partial
    };

public:
    TPartitionDatabase(NKikimr::NTable::TDatabase& database)
        : NKikimr::NIceDb::TNiceDb(database)
    {}

    void InitSchema();

    //
    // Meta
    //
    void WriteMeta(const TString& meta);
    bool ReadMeta(TMaybe<TString>& meta);
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
