#pragma once

#include "blobsource.h"
#include "tabletlog.h"

namespace NKikimr::NTable {
    class TDatabase;
    struct IPages;
}

namespace NKikimr::NPDiskTool {

struct TTabletBootStats {
    bool HasSnapshot = false;
    ui64 SnapshotSerial = 0;
    ui64 AlterEntries = 0;
    ui64 AlterSkipped = 0;
    ui64 Switches = 0;
    ui64 SwitchesSkipped = 0;
    ui64 RedoEntries = 0;
    ui64 RedoSkipped = 0;
    ui64 BundlesLoaded = 0;
    ui64 BundlesDropped = 0;
    ui64 TxStatusLoaded = 0;
    ui64 TxStatusDropped = 0;
    ui64 AnnexBlobs = 0;
    ui64 AnnexMissing = 0;
    ui64 PagesRead = 0;
    ui64 PagesMissing = 0;
    ui64 PagesCorrupt = 0;
    ui64 LoanEntries = 0;
    ui64 GcEntries = 0;
};

// The flat executor boot sequence without the actor system: the same classification of log blobs, the
// same snapshot handling, the same order of scheme, switch, bundle and redo application, with the
// shared cache and BlobStorage replaced by a blob store and every read done inline.
//
// Every unit of work is independent, so a log entry, a bundle or a page that cannot be recovered is
// reported and skipped rather than ending the run.
class TTabletBoot {
public:
    TTabletBoot(TBlobStore& store, ui64 tabletId, TIssueLog& issues);
    ~TTabletBoot();

    bool Run(const TTabletLogHistory& history);

    // Valid after a successful Run; the env has to stay alive for as long as the database is read.
    NTable::TDatabase& Database();
    NTable::IPages& Pages();

    const TTabletBootStats& Stats() const;

private:
    class TImpl;
    THolder<TImpl> Impl;
};

} // namespace NKikimr::NPDiskTool
