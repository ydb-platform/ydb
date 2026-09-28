#include "blobsource.h"

#include <util/folder/path.h>
#include <util/generic/algorithm.h>
#include <util/stream/file.h>
#include <util/string/cast.h>

namespace NKikimr::NPDiskTool {

bool ParseExportedBlobName(const TString& name, TLogoBlobID& id, ui32& partId) {
    const size_t close = name.find(']');
    if (close == TString::npos) {
        return false;
    }
    TString error;
    if (!TLogoBlobID::Parse(id, name.substr(0, close + 1), error)) {
        return false;
    }
    id = id.FullID();
    TStringBuf rest(name.data() + close + 1, name.size() - close - 1);
    if (!rest.SkipPrefix(".part")) {
        return false;
    }
    const TStringBuf part = rest.NextTok('.');
    if (!TryFromString(part, partId) || partId == 0 || partId > TLogoBlobID::MaxPartId) {
        return false;
    }
    if (rest) {
        // `.copyM` marks a part whose on-disk copies disagreed, so every one of them was exported.
        ui32 copy = 0;
        if (!rest.SkipPrefix("copy") || !TryFromString(rest, copy) || copy == 0) {
            return false;
        }
    }
    return true;
}

TBlobStore::TBlobStore(TMaybe<TErasureType> erasure, TIssueLog& issues)
    : Erasure(erasure)
    , Issues(issues)
    , Repeated("blob-store", "blob")
{}

bool TBlobStore::AddDirectory(const TString& path) {
    TFsPath dir(path);
    if (!dir.Exists()) {
        Issues.Error("blob-store", TStringBuilder() << "Blob directory " << path << " does not exist");
        return false;
    }
    if (!dir.IsDirectory()) {
        Issues.Error("blob-store", TStringBuilder() << path << " is not a directory");
        return false;
    }

    TVector<TFsPath> children;
    dir.List(children);
    for (const auto& child : children) {
        if (child.IsDirectory()) {
            continue;
        }
        ++Stats_.FilesScanned;
        TLogoBlobID id;
        ui32 partId = 0;
        if (!ParseExportedBlobName(child.GetName(), id, partId)) {
            ++Stats_.FilesSkipped;
            continue;
        }
        TString body = TUnbufferedFileInput(child.GetPath()).ReadAll();
        auto& entry = Blobs[id];
        if (entry.Parts.size() < partId) {
            entry.Parts.resize(partId);
        }
        auto& candidates = entry.Parts[partId - 1];
        if (std::find(candidates.begin(), candidates.end(), body) == candidates.end()) {
            if (!candidates.empty()) {
                // Two directories, or two on-disk copies, hold different bytes under the same part id.
                // The first one wins, so directory order on the command line is the precedence.
                ++Stats_.DisagreeingParts;
                Repeated.Add("Copies of a part disagree, the first one is used", id.ToString());
            }
            candidates.push_back(std::move(body));
        }
        ++Stats_.PartFiles;
        Dirty = true;
    }

    if (Dirty) {
        Sorted.clear();
        Sorted.reserve(Blobs.size());
        for (const auto& [id, entry] : Blobs) {
            Sorted.push_back(id);
        }
        Sort(Sorted);
        Stats_.Blobs = Sorted.size();
        Dirty = false;
    }
    return true;
}

TVector<TLogoBlobID> TBlobStore::Range(const TLogoBlobID& from, const TLogoBlobID& to) const {
    TVector<TLogoBlobID> result;
    auto it = LowerBound(Sorted.begin(), Sorted.end(), from);
    for (; it != Sorted.end() && !(to < *it); ++it) {
        result.push_back(*it);
    }
    return result;
}

TBlobStore::TEntry* TBlobStore::Find(const TLogoBlobID& id) {
    const auto it = Blobs.find(id.FullID());
    return it == Blobs.end() ? nullptr : &it->second;
}

const TBlobStore::TEntry* TBlobStore::Find(const TLogoBlobID& id) const {
    const auto it = Blobs.find(id.FullID());
    return it == Blobs.end() ? nullptr : &it->second;
}

ui64 TBlobStore::ExpectedPartSize(const TLogoBlobID& id, ui32 partId) const {
    if (!Erasure) {
        return id.BlobSize();
    }
    if (Erasure->GetErasure() == TErasureType::ErasureMirror3of4 && partId == 3) {
        // The third part of mirror-3of4 is a marker without a body.
        return 0;
    }
    if (!TErasureType::IsCrcModeValid(id.CrcMode())) {
        return 0;
    }
    return Erasure->PartSize(TErasureType::ECrcMode(id.CrcMode()), id.BlobSize());
}

ui32 TBlobStore::UsableParts(const TLogoBlobID& id, const TEntry& entry) const {
    ui32 count = 0;
    for (size_t idx = 0; idx < entry.Parts.size(); ++idx) {
        if (entry.Parts[idx].empty()) {
            continue;
        }
        const ui64 expected = ExpectedPartSize(id, idx + 1);
        if (expected && entry.Parts[idx].front().size() == expected) {
            ++count;
        }
    }
    return count;
}

bool TBlobStore::CanRestore(const TLogoBlobID& id) const {
    const TEntry* entry = Find(id);
    if (!entry) {
        return false;
    }
    if (entry->Restored) {
        return true;
    }
    if (entry->Failed || !id.BlobSize()) {
        return false;
    }
    if (!Erasure) {
        return UsableParts(id, *entry) > 0;
    }
    if (entry->Parts.size() > Erasure->TotalPartCount()) {
        return false;
    }
    return UsableParts(id, *entry) >= Erasure->MinimalRestorablePartCount();
}

bool TBlobStore::Restore(const TLogoBlobID& id, TEntry& entry) {
    if (entry.Restored) {
        return true;
    }
    if (entry.Failed) {
        return false;
    }

    auto fail = [&](const TString& why) {
        entry.Failed = true;
        ++Stats_.Unrecoverable;
        Unrecoverable_.push_back(id);
        Repeated.Add(why, id.ToString());
        TVector<TVector<TString>>().swap(entry.Parts);
        return false;
    };

    if (!id.BlobSize()) {
        return fail("Blob has zero size");
    }

    if (!Erasure) {
        // Nothing to decode with, so only a part that already holds the whole body is of use.
        for (size_t idx = 0; idx < entry.Parts.size(); ++idx) {
            if (!entry.Parts[idx].empty() && entry.Parts[idx].front().size() == id.BlobSize()) {
                entry.Whole = entry.Parts[idx].front();
                entry.Restored = true;
                break;
            }
        }
        if (!entry.Restored) {
            return fail("No part spans the whole blob and no --erasure was given");
        }
    } else {
        const ui32 total = Erasure->TotalPartCount();
        TVector<TRope> parts(total);
        ui32 have = 0;
        for (size_t idx = 0; idx < entry.Parts.size(); ++idx) {
            if (entry.Parts[idx].empty()) {
                continue;
            }
            if (idx >= total) {
                ++Stats_.WrongSizeParts;
                Repeated.Add("Part id is outside the erasure", id.ToString());
                continue;
            }
            // Every abort inside ErasureRestore is about part sizes, so they are all checked here:
            // a part of the wrong length is treated as absent.
            const ui64 expected = ExpectedPartSize(id, idx + 1);
            if (!expected || entry.Parts[idx].front().size() != expected) {
                ++Stats_.WrongSizeParts;
                Repeated.Add("Part size does not match the erasure", id.ToString());
                continue;
            }
            parts[idx] = TRope(entry.Parts[idx].front());
            ++have;
        }
        if (have < Erasure->MinimalRestorablePartCount()) {
            return fail(TStringBuilder() << "Only " << have << " usable part(s), "
                << Erasure->MinimalRestorablePartCount() << " needed");
        }
        TRope whole;
        try {
            ErasureRestore(TErasureType::ECrcMode(id.CrcMode()), *Erasure, id.BlobSize(), &whole, parts, 0);
        } catch (...) {
            return fail(TStringBuilder() << "Erasure restore failed: " << CurrentExceptionMessage());
        }
        if (whole.size() != id.BlobSize()) {
            return fail("Erasure restore produced a body of the wrong size");
        }
        entry.Whole = whole.ConvertToString();
        entry.Restored = true;
    }

    ++Stats_.Restored;
    Stats_.BytesRestored += entry.Whole.size();
    // The parts are as big as the body, and nothing reads them again once the body is there.
    TVector<TVector<TString>>().swap(entry.Parts);
    return true;
}

const TString* TBlobStore::Get(const TLogoBlobID& id) {
    TEntry* entry = Find(id);
    if (!entry) {
        return nullptr;
    }
    return Restore(id.FullID(), *entry) ? &entry->Whole : nullptr;
}

bool TBlobStore::Get(const NPageCollection::TLargeGlobId& largeGlobId, TString& out) {
    if (!largeGlobId || !largeGlobId.Lead || !largeGlobId.Lead.BlobSize()
            || largeGlobId.Lead.BlobSize() > largeGlobId.Bytes) {
        // TLargeGlobIdRestoreState divides by the lead size and asserts on the rest of this.
        return false;
    }
    NPageCollection::TLargeGlobIdRestoreState state(largeGlobId);
    for (const TLogoBlobID& id : largeGlobId.Blobs()) {
        const TString* body = Get(id);
        if (!body || body->size() != id.BlobSize()) {
            return false;
        }
        state.Apply(id, *body);
    }
    if (!state) {
        return false;
    }
    out = state.ExtractString();
    return out.size() == largeGlobId.Bytes;
}

void TBlobStore::FlushIssues() {
    Repeated.Flush(Issues, "warning");
    Repeated = TRepeatedIssues("blob-store", "blob");
}

} // namespace NKikimr::NPDiskTool
