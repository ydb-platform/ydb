#pragma once

#include "issues.h"

#include <ydb/core/erasure/erasure.h>
#include <ydb/core/tablet_flat/flat_sausage_solid.h>

#include <util/generic/hash.h>

namespace NKikimr::NPDiskTool {

struct TBlobStoreStats {
    ui64 FilesScanned = 0;
    ui64 FilesSkipped = 0;   // names that are not [id].partN[.copyM]
    ui64 PartFiles = 0;
    ui64 Blobs = 0;
    ui64 DisagreeingParts = 0;
    ui64 WrongSizeParts = 0;
    ui64 Restored = 0;
    ui64 Unrecoverable = 0;
    ui64 BytesRestored = 0;
};

// Whole blobs recovered from what `export-blob` wrote. One export directory holds the parts a single
// VDisk happened to keep, so several directories are merged here and the erasure code fills the gaps.
class TBlobStore {
public:
    // Without an erasure species only parts that already span the whole blob are usable, which covers
    // the mirror flavours and block-4-1 style single-part blobs but nothing else.
    TBlobStore(TMaybe<TErasureType> erasure, TIssueLog& issues);

    bool AddDirectory(const TString& path);

    // Ids of every blob that has at least one part, sorted, part id stripped.
    const TVector<TLogoBlobID>& Ids() const {
        return Sorted;
    }

    // Blobs of `tabletId` on `channel` within [from, to], in log order.
    TVector<TLogoBlobID> Range(const TLogoBlobID& from, const TLogoBlobID& to) const;

    // Enough parts of the right size to reassemble the blob, without doing the work.
    bool CanRestore(const TLogoBlobID& id) const;

    // Restores on first use and keeps the result; nullptr when the blob cannot be reassembled.
    const TString* Get(const TLogoBlobID& id);

    // Concatenates the whole run of blobs a TLargeGlobId spans.
    bool Get(const NPageCollection::TLargeGlobId& largeGlobId, TString& out);

    const TBlobStoreStats& Stats() const {
        return Stats_;
    }

    const TVector<TLogoBlobID>& Unrecoverable() const {
        return Unrecoverable_;
    }

    // Summarizes the per-blob complaints that were counted instead of reported one by one.
    void FlushIssues();

private:
    struct TEntry {
        TVector<TVector<TString>> Parts; // by part index, several entries when copies disagreed
        TString Whole;
        bool Restored = false;
        bool Failed = false;
    };

    TEntry* Find(const TLogoBlobID& id);
    const TEntry* Find(const TLogoBlobID& id) const;
    bool Restore(const TLogoBlobID& id, TEntry& entry);
    ui64 ExpectedPartSize(const TLogoBlobID& id, ui32 partId) const;
    ui32 UsableParts(const TLogoBlobID& id, const TEntry& entry) const;

    TMaybe<TErasureType> Erasure;
    TIssueLog& Issues;
    TRepeatedIssues Repeated;
    THashMap<TLogoBlobID, TEntry> Blobs;
    TVector<TLogoBlobID> Sorted;
    TVector<TLogoBlobID> Unrecoverable_;
    TBlobStoreStats Stats_;
    bool Dirty = false; // Sorted needs rebuilding
};

// Splits `[id].partN[.copyM]` into its pieces. Public for the tests and for anything else that has to
// make sense of an export directory.
bool ParseExportedBlobName(const TString& name, TLogoBlobID& id, ui32& partId);

} // namespace NKikimr::NPDiskTool
