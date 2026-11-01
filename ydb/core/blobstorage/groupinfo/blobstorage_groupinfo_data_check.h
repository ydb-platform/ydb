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
        const ui32 dataParts = type.DataParts();
        const ui32 totalParts = type.TotalPartCount();
        const auto crcMode = static_cast<TErasureType::ECrcMode>(id.CrcMode());
        const ui64 partSize = type.PartSize(TLogoBlobID(id, 1));
        Y_ABORT_UNLESS(partsData.Parts.size() == totalParts);

        TPartsState partsState;

        struct TSeenPart {
            TRope Data;
            std::vector<ui32> DiskIdxs;
        };
        TStackVec<std::vector<TSeenPart>, TErasureType::MaxTotalParts> seenParts(totalParts);

        // find all distinct copies of each part
        for (ui32 partId = 0; partId < totalParts; ++partId) {
            auto& seen = seenParts[partId];
            for (const auto& [diskIdx, data] : partsData.Parts[partId]) {
                if (data.size() != partSize || !CheckCrcAtTheEnd(crcMode, data)) {
                    partsState.IsOk = false;
                    partsState.DataInfo = TStringBuilder() << "ERROR: incorrect size or CRC for part " << partId + 1
                        << " on disk " << diskIdx << separator;
                    return partsState;
                }
                bool isNew = true;
                for (auto& seenPart : seen) {
                    if (!TRope::Compare(data, seenPart.Data)) {
                        seenPart.DiskIdxs.push_back(diskIdx);
                        isNew = false;
                        break;
                    }
                }
                if (isNew) {
                    seen.push_back({data, {diskIdx}});
                }
            }
        }

        // checking layout
        TStringStream layoutReport;
        layoutReport << "Layout info:" << separator;

        TStringStream str;
        bool hasUnequalParts = false;
        for (ui32 partId = 0; partId < totalParts; ++partId) {
            const auto& seen = seenParts[partId];
            if (seen.size() > 1) {
                hasUnequalParts = true;
            }
            str << "part " << partId + 1 << ": ";
            ui32 ver = 0;
            for (const auto& seenPart : seen) {
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
        }

        layoutReport << str.Str();
        if (hasUnequalParts) {
            partsState.IsOk = false;
            layoutReport << "ERROR: There are unequal parts" << separator;
        }
        partsState.DataInfo = layoutReport.Str();

        // checking erasure
        TStringStream erasureReport;
        erasureReport << "Erasure info:" << separator;

        std::vector<ui32> partIds;
        partIds.reserve(totalParts);
        bool erasureError = false;

        auto checkCombination = [&]() {
            TStackVec<const TSeenPart*, TErasureType::MaxTotalParts> versions(dataParts);
            // iterate over combinations of part versions
            auto checkVersions = [&](auto&& self, ui32 part) -> void {
                if (part < dataParts) {
                    for (const auto& seen : seenParts[partIds[part]]) {
                        versions[part] = &seen;
                        self(self, part + 1);
                    }
                    return;
                }

                TStackVec<TRope, TErasureType::MaxTotalParts> data(totalParts);
                for (ui32 part = 0; part < dataParts; ++part) {
                    data[partIds[part]] = versions[part]->Data;
                }

                ui32 restoreMask = 0;
                restoreMask |= (1 << partIds[dataParts]);
                if (partIds.size() == totalParts) {
                    restoreMask |= (1 << partIds[dataParts + 1]);
                }

                ErasureRestore(crcMode, type, id.BlobSize(), nullptr, data, restoreMask);

                auto checkOnePart = [&](ui32 partId) {
                    for (const auto& seen : seenParts[partId]) {
                        TStringStream str;
                        str << "{ ";
                        for (ui32 part = 0; part < dataParts; ++part) {
                            str << "part " << partIds[part] + 1 << " disks [ ";
                            for (const auto& diskIdx : versions[part]->DiskIdxs) {
                                str << diskIdx << " ";
                            }
                            str << "]; ";
                        }
                        str << "} CHECK part " << partId + 1 << " disks [ ";
                        for (const auto& diskIdx : seen.DiskIdxs) {
                            str << diskIdx << " ";
                        }
                        str << "] -> ";

                        int cmp = TRope::Compare(seen.Data, data[partId]);
                        if (cmp) {
                            erasureError = true;
                        } else {
                            str << "OK" << separator;
                            erasureReport << str.Str(); // report only succesful restore
                        }
                    }
                };

                checkOnePart(partIds[dataParts]);
                if (partIds.size() == totalParts) {
                    checkOnePart(partIds[dataParts + 1]);
                }
            };
            checkVersions(checkVersions, 0);
        };

        for (ui32 partId = 0; partId < totalParts; ++partId) {
            if (!seenParts[partId].empty()) {
                partIds.push_back(partId);
            }
        }
        if (partIds.size() <= dataParts || !partSize) { // no redundant part bytes to check
            return partsState;
        }

        // fast path: there's no unequal parts; if simple check is ok, return
        if (!hasUnequalParts) {
            checkCombination();
            if (!erasureError) {
                partsState.DataInfo += erasureReport.Str();
                return partsState;
            }
        }

        if (partIds.size() == totalParts - 1) {
            checkCombination();
        } else { // partIds.size() == totalParts
            // iterate over different combinations to find good parts
            for (ui8 gap1 = 0; gap1 < totalParts - 1; ++gap1) {
                for (ui8 gap2 = gap1 + 1; gap2 < totalParts; ++gap2) {
                    ui8 idx = 0;
                    for (ui8 p = 0; p < totalParts; ++p) {
                        if (p == gap1) {
                            partIds[dataParts] = p;
                        } else if (p == gap2) {
                            partIds[dataParts + 1] = p;
                        } else {
                            partIds[idx] = p;
                            ++idx;
                        }
                    }
                    checkCombination();
                }
            }
        }

        if (erasureError) {
            partsState.IsOk = false;
            erasureReport << "ERROR: There are erasure restore fails" << separator;
        }

        partsState.DataInfo += erasureReport.Str();
        return partsState;
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
