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

class TDataIntegrityCheckerBlock42 : public TDataIntegrityCheckerBase {
public:
    using TDataIntegrityCheckerBase::TDataIntegrityCheckerBase;

    TPartsState GetDataState(const TLogoBlobID& id, const TPartsData& partsData, char separator) const override {
        Y_ABORT_UNLESS(partsData.Parts.size() == 6);

        TPartsState partsState;

        struct TSeenPart {
            TRope Data;
            std::vector<ui32> DiskIdxs;
        };
        std::array<std::vector<TSeenPart>, 6> seenParts;

        // find all distinct copies of each part
        for (ui32 partId = 0; partId < 6; ++partId) {
            auto& seen = seenParts[partId];
            for (const auto& [diskIdx, data] : partsData.Parts[partId]) {
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
        for (ui32 partId = 0; partId < 6; ++partId) {
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
        partIds.reserve(6);
        bool erasureError = false;

        TErasureType::ECrcMode crcMode = (TErasureType::ECrcMode)id.CrcMode();

        auto checkCombination = [&]() {
            // iterate over combinations of part versions
            for (const auto& seen0 : seenParts[partIds[0]]) {
            for (const auto& seen1 : seenParts[partIds[1]]) {
            for (const auto& seen2 : seenParts[partIds[2]]) {
            for (const auto& seen3 : seenParts[partIds[3]]) {
                std::array<TRope, 6> data;
                data[partIds[0]] = seen0.Data;
                data[partIds[1]] = seen1.Data;
                data[partIds[2]] = seen2.Data;
                data[partIds[3]] = seen3.Data;

                ui32 restoreMask = 0;
                restoreMask |= (1 << partIds[4]);
                if (partIds.size() == 6) {
                    restoreMask |= (1 << partIds[5]);
                }

                ErasureRestore(crcMode, TErasureType::Erasure4Plus2Block, id.BlobSize(), nullptr, data, restoreMask);

                std::array<std::vector<ui32>, 4> diskIdxs{
                    seen0.DiskIdxs, seen1.DiskIdxs, seen2.DiskIdxs, seen3.DiskIdxs};

                auto checkOnePart = [&](ui32 partId) {
                    for (const auto& seen : seenParts[partId]) {
                        TStringStream str;
                        str << "{ ";
                        for (ui32 part = 0; part < 4; ++part) {
                            str << "part " << partIds[part] + 1 << " disks [ ";
                            for (const auto& diskIdx : diskIdxs[part]) {
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

                checkOnePart(partIds[4]);
                if (partIds.size() == 6) {
                    checkOnePart(partIds[5]);
                }
            }}}}
        };

        for (ui32 partId = 0; partId < 6; ++partId) {
            if (!seenParts[partId].empty()) {
                partIds.push_back(partId);
            }
        }
        if (partIds.size() <= 4) { // 4 or less parts total, nothing to check
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

        if (partIds.size() == 5) {
            checkCombination();
        } else { // partIds.size() == 6
            // iterate over different combinations to find good parts
            for (ui8 gap1 = 0; gap1 < 5; ++gap1) {
                for (ui8 gap2 = gap1 + 1; gap2 < 6; ++gap2) {
                    ui8 idx = 0;
                    for (ui8 p = 0; p < 6; ++p) {
                        if (p == gap1) {
                            partIds[4] = p;
                        } else if (p == gap2) {
                            partIds[5] = p;
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
