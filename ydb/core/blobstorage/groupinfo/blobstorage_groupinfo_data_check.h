#include "blobstorage_groupinfo.h"

namespace NKikimr {

class TDataIntegrityCheckerBase : public TBlobStorageGroupInfo::IDataIntegrityChecker {
protected:
    const TBlobStorageGroupInfo::TTopology *Top;

public:
    explicit TDataIntegrityCheckerBase(const TBlobStorageGroupInfo::TTopology *top)
        : Top(top)
    {}
};

class TDataIntegrityCheckerTrivial : public TDataIntegrityCheckerBase {
public:
    using TDataIntegrityCheckerBase::TDataIntegrityCheckerBase;

    TPartsState GetDataState(const TLogoBlobID& id, const TPartsData& partsData, char separator) const override {
        Y_UNUSED(id);
        Y_UNUSED(partsData);
        Y_UNUSED(separator);
        return {};
    }
};

class TDataIntegrityCheckerParityBlock : public TDataIntegrityCheckerBase {
public:
    using TDataIntegrityCheckerBase::TDataIntegrityCheckerBase;

    TPartsState GetDataState(const TLogoBlobID& id, const TPartsData& partsData, char separator) const override {
        const auto& type = Top->GType;
        const ui32 total = type.TotalPartCount();
        const ui32 required = type.DataParts();
        TPartsState state;
        TStringStream report;
        auto fail = [&](TStringBuf reason, ui32 partId = 0) {
            state.IsOk = false;
            report << "ERROR: " << reason;
            if (partId) {
                report << " part " << partId;
            }
            report << separator;
            state.DataInfo = report.Str();
            return state;
        };

        if (partsData.Parts.size() != total || total > MaxTotalPartCount) {
            return fail("invalid part count");
        }
        if (!TErasureType::IsCrcModeValid(id.CrcMode())) {
            return fail("invalid CRC mode");
        }
        const auto crcMode = static_cast<TErasureType::ECrcMode>(id.CrcMode());
        const ui64 expectedSize = type.TErasureType::PartSize(crcMode, id.BlobSize());
        std::array<TRope, MaxTotalPartCount> basis;
        ui32 available = 0;
        ui32 restoreMask = 0;

        // Validate every physical copy before entering the codec. The report has
        // at most one line per PartId, independent of the number of copies.
        for (ui32 part = 0; part < total; ++part) {
            const auto& copies = partsData.Parts[part];
            if (copies.empty()) {
                continue;
            }
            const TRope& first = copies.front().second;
            for (const auto& [diskIdx, data] : copies) {
                Y_UNUSED(diskIdx);
                if (data.size() != expectedSize) {
                    return fail("invalid size", part + 1);
                }
                bool validCrc;
                if (crcMode == TErasureType::CrcModeWholePart && expectedSize == sizeof(ui32)) {
                    // Parity codes retain a CRC suffix for an empty blob. The
                    // generic helper requires at least one payload byte.
                    ui32 storedCrc;
                    auto it = data.Begin();
                    it.ExtractPlainDataAndAdvance(&storedCrc, sizeof(storedCrc));
                    validCrc = storedCrc == 0; // CRC32C of an empty payload.
                } else {
                    validCrc = CheckCrcAtTheEnd(crcMode, data);
                }
                if (!validCrc) {
                    return fail("invalid CRC", part + 1);
                }
                if (TRope::Compare(first, data)) {
                    return fail("unequal copies", part + 1);
                }
            }
            report << "part " << part + 1 << ": " << copies.size() << " equal copies" << separator;
            if (available++ < required) {
                basis[part] = first;
            } else {
                restoreMask |= 1u << part;
            }
        }

        if (available <= required) {
            report << "No independent redundancy: checked copies, sizes and CRC only" << separator;
        } else if (!id.BlobSize()) {
            // Empty headerless parts cannot represent presence in the codec's
            // rope API. Sizes and CRC already validate an empty codeword.
            report << "Empty codeword OK" << separator;
        } else {
            // An MDS K+2 codeword is determined by any K distinct parts. Checking
            // every other observed part against one basis is sufficient.
            ErasureRestore(crcMode, type, id.BlobSize(), nullptr,
                std::span<TRope>(basis.data(), total), restoreMask);
            for (ui32 part = 0; part < total; ++part) {
                if ((restoreMask >> part & 1u) && TRope::Compare(basis[part], partsData.Parts[part].front().second)) {
                    return fail("inconsistent codeword", part + 1);
                }
            }
            report << "Codeword OK" << separator;
        }
        state.DataInfo = report.Str();
        return state;
    }
};


class TDataIntegrityCheckerMirror : public TDataIntegrityCheckerBase {
private:
    virtual ui32 DataPartsCount() const = 0;

public:
    using TDataIntegrityCheckerBase::TDataIntegrityCheckerBase;

    TPartsState GetDataState(const TLogoBlobID& id, const TPartsData& partsData, char separator) const override {
        Y_UNUSED(id);
        Y_ABORT_UNLESS(partsData.Parts.size() == 3);

        TPartsState partsState;

        struct TSeenPart {
            TRope Data;
            std::vector<ui32> DiskIdxs;
        };
        std::vector<TSeenPart> seenParts;

        // find all distinct copies of the blob
        for (ui32 partId = 0; partId < DataPartsCount(); ++partId) {
            for (const auto& [diskIdx, data] : partsData.Parts[partId]) {
                bool isNew = true;
                for (auto& seenPart : seenParts) {
                    if (!TRope::Compare(data, seenPart.Data)) {
                        seenPart.DiskIdxs.push_back(diskIdx);
                        isNew = false;
                        break;
                    }
                }
                if (isNew) {
                    seenParts.push_back({data, {diskIdx}});
                }
            }
        }

        TStringStream layoutReport;
        layoutReport << "Layout info:" << separator;

        TStringStream str;
        bool hasUnequalParts = (seenParts.size() > 1);
        ui32 ver = 0;
        for (const auto& seenPart : seenParts) {
            if (ver > 0) {
                str << ", ";
            }
            str << "ver" << ver << " disks [ ";
            for (const auto& diskIdx : seenPart.DiskIdxs) {
                str << diskIdx << " ";
            }
            str << "]";
            ++ver;
        }
        str << separator;
        layoutReport << str.Str();

        if (hasUnequalParts) {
            partsState.IsOk = false;
            layoutReport << "ERROR: There are unequal parts" << separator;
        }
        partsState.DataInfo = layoutReport.Str();

        return partsState;
    }
};

class TDataIntegrityCheckerMirror3dc : public TDataIntegrityCheckerMirror {
private:
    ui32 DataPartsCount() const override { return 3; }

public:
    using TDataIntegrityCheckerMirror::TDataIntegrityCheckerMirror;
};

class TDataIntegrityCheckerMirror3of4 : public TDataIntegrityCheckerMirror {
private:
    ui32 DataPartsCount() const override { return 2; }

public:
    using TDataIntegrityCheckerMirror::TDataIntegrityCheckerMirror;
};

} // NKikimr
