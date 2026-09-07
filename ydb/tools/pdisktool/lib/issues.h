#pragma once

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_data.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_defs.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_state.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_signature.h>
#include <ydb/core/blobstorage/base/blobstorage_vdiskid.h>
#include <ydb/core/base/logoblob.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/map.h>
#include <util/generic/maybe.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

#include <ydb/tools/pdisktool/proto/pdisktool.pb.h>

namespace NKikimr::NPDiskTool {

using NPDisk::TDiskFormat;
using NPDisk::TDataSectorFooter;
using NPDisk::TKey;
using NPDisk::TMainKey;
using NPDisk::TOwner;
using NKikimr::TChunkIdx;
using NKikimr::TLogSignature;
using NPDisk::TSysLogRecord;
using NPDisk::TChunkInfo;
using NPDisk::TSysLogFirstNoncesToKeep;
using NPDisk::TChunkTrimInfo;
using NPDisk::TCommitRecordFooter;
using NPDisk::TLogPageHeader;
using NPDisk::TFirstLogPageHeader;
using NPDisk::TLogRecordHeader;
using NPDisk::TNextLogChunkReference2;
using NPDisk::TNextLogChunkReference3;
using NPDisk::TNonceJumpLogPageHeader1;
using NPDisk::TNonceJumpLogPageHeader2;
using NPDisk::TPDiskStreamCypher;
using NPDisk::TPDiskHashCalculator;
using NPDisk::TChunkState;
using NPDisk::EOwner;
using NPDisk::IsOwnerAllocated;
using NPDisk::IsOwnerUser;

struct TIssue {
    TString Severity; // info, warning, error
    TString Location;
    TString Message;
    bool Guessed = false;
};

class TIssueLog {
public:
    TVector<TIssue> Items;
    bool Strict = false;
    bool StrictTriggered = false;

    void Add(TString severity, TString location, TString message, bool guessed = false) {
        Items.push_back(TIssue{std::move(severity), std::move(location), std::move(message), guessed});
        if (Strict && Items.back().Severity == "error") {
            StrictTriggered = true;
        }
    }

    void Error(TString location, TString message, bool guessed = false) {
        Add("error", std::move(location), std::move(message), guessed);
    }

    void Warning(TString location, TString message, bool guessed = false) {
        Add("warning", std::move(location), std::move(message), guessed);
    }

    void Info(TString location, TString message, bool guessed = false) {
        Add("info", std::move(location), std::move(message), guessed);
    }

    void FillProto(google::protobuf::RepeatedPtrField<NKikimr::NPdiskTool::TIssue>* out) const {
        for (const auto& i : Items) {
            auto* p = out->Add();
            p->SetSeverity(i.Severity);
            p->SetLocation(i.Location);
            p->SetMessage(i.Message);
            p->SetGuessed(i.Guessed);
        }
    }

    bool HasErrors() const {
        for (const auto& i : Items) {
            if (i.Severity == "error") {
                return true;
            }
        }
        return false;
    }
};

// A damaged disk hits the same inconsistency in millions of records, and anything reported once per
// record buries the real findings. Repeated conditions are counted here and summarized once, keeping
// the last position as an entry point for a manual look.
class TRepeatedIssues {
    struct TEntry {
        ui64 Count = 0;
        TString Last;
    };
    TMap<TString, TEntry> Entries;
    TString Location;
    TString PositionName;

public:
    TRepeatedIssues(TString location = "hull", TString positionName = "lsn")
        : Location(std::move(location))
        , PositionName(std::move(positionName))
    {}

    void Add(const TString& what, ui64 position) {
        Add(what, ToString(position));
    }

    // For positions that are not a number, a blob id being the common case.
    void Add(const TString& what, TString position) {
        auto& e = Entries[what];
        ++e.Count;
        e.Last = std::move(position);
    }

    bool Empty() const {
        return Entries.empty();
    }

    void Flush(TIssueLog& issues, const TString& severity) const {
        for (const auto& [what, e] : Entries) {
            issues.Add(severity, Location, TStringBuilder() << what << ": " << e.Count
                << " record(s), last " << PositionName << "# " << e.Last);
        }
    }
};

// Bytes actually available for a claim of `want` starting at `pos` within `size`. Every size read off
// the disk goes through here before it reaches an allocation or a memcpy.
inline ui64 ClampSpan(ui64 pos, ui64 want, ui64 size) {
    return pos >= size ? 0 : Min<ui64>(want, size - pos);
}

inline TString SignatureName(ui8 signature) {
    return TLogSignature(signature).ToString();
}

inline TString OwnerName(ui32 owner) {
    switch (owner) {
        case EOwner::OwnerSystem: return "System";
        case EOwner::OwnerUnallocated: return "Unallocated";
        case EOwner::OwnerMetadata: return "Metadata";
        case EOwner::OwnerSystemLog: return "SystemLog";
        case EOwner::OwnerSystemReserve: return "SystemReserve";
        case EOwner::OwnerCommonStaticLog: return "CommonStaticLog";
        case EOwner::OwnerUnallocatedTrimmed: return "UnallocatedTrimmed";
        case EOwner::OwnerLocked: return "Locked";
        default:
            if (IsOwnerUser(owner)) {
                return TStringBuilder() << "User";
            }
            return TStringBuilder() << "Owner" << owner;
    }
}

inline TString CommitStateName(TChunkState::ECommitState s) {
    switch (s) {
        case TChunkState::FREE: return "FREE";
        case TChunkState::DATA_RESERVED_DELETE_IN_PROGRESS: return "DATA_RESERVED_DELETE_IN_PROGRESS";
        case TChunkState::DATA_COMMITTED_DELETE_IN_PROGRESS: return "DATA_COMMITTED_DELETE_IN_PROGRESS";
        case TChunkState::DATA_RESERVED: return "DATA_RESERVED";
        case TChunkState::DATA_COMMITTED: return "DATA_COMMITTED";
        case TChunkState::DATA_ON_QUARANTINE: return "DATA_ON_QUARANTINE";
        case TChunkState::DATA_COMMITTED_DELETE_ON_QUARANTINE: return "DATA_COMMITTED_DELETE_ON_QUARANTINE";
        case TChunkState::LOG_RESERVED: return "LOG_RESERVED";
        case TChunkState::LOG_COMMITTED: return "LOG_COMMITTED";
        case TChunkState::DATA_RESERVED_DELETE_ON_QUARANTINE: return "DATA_RESERVED_DELETE_ON_QUARANTINE";
        case TChunkState::DATA_DECOMMITTED: return "DATA_DECOMMITTED";
        case TChunkState::DATA_RESERVED_DECOMMIT_IN_PROGRESS: return "DATA_RESERVED_DECOMMIT_IN_PROGRESS";
        case TChunkState::DATA_COMMITTED_DECOMMIT_IN_PROGRESS: return "DATA_COMMITTED_DECOMMIT_IN_PROGRESS";
        case TChunkState::LOCKED: return "LOCKED";
        default: return TStringBuilder() << "UNKNOWN(" << (ui32)s << ")";
    }
}

} // namespace NKikimr::NPDiskTool
