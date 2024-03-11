#pragma once
#include <ydb/core/blobstorage/defs.h>
#include <ydb/core/erasure/erasure.h>
#include <ydb/core/base/logoblob.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <util/generic/list.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>

namespace NKikimr {

static constexpr ui32 BlobProtobufHeaderMaxSize = 80;

struct TBloblStorageErasureParameters;

struct TBlobStorageGroupType : public TErasureType {

    TBlobStorageGroupType(TErasureType::EErasureSpecies s = TErasureType::ErasureNone)
        : TErasureType(s)
    {}

    struct TPartLayout {
        TStackVec<ui32, 32> VDiskPartMask;
        ui32 VDiskMask;
        ui32 SlowVDiskMask;

        TPartLayout()
            : VDiskMask(0)
            , SlowVDiskMask(0)
        {}

        TString ToString() const {
            TStringStream str;
            str << "{TPartLayout ";
            str << " Sessions {";
            bool isFirst = true;
            for (ui32 i = 0; i < 32; ++i) {
                bool isPresent = (VDiskMask & (1 << i));
                if (isPresent) {
                    str << (isFirst ? "" : ", ") << i;
                }
                isFirst = false;
            }
            str << "}";

            isFirst = true;
            for (ui32 i = 0; i < 32; ++i) {
                bool isPresent = (VDiskMask & (1 << i));
                if (isPresent) {
                    str << (isFirst ? "" : ", ") << "VDiskPartMask[" << i << "]# {";
                    bool isSlow = (SlowVDiskMask & (1 << i));
                    if (isSlow) {
                        str << "SlowDisk ";
                    }
                    bool isFirstPart = true;
                    for (ui32 partIdx = 0; partIdx < 32; ++partIdx) {
                        bool isPartPresent = (VDiskPartMask[i] & (1 << partIdx));
                        if (isPartPresent) {
                            str << (isFirstPart ? "" : ", ") << partIdx;
                            isFirstPart = false;
                        }
                    }
                    str << "}";
                    isFirst = false;
                }
            }
            str << "}";
            return str.Str();
        }
    };

    struct TPartPlacement {
        struct TVDiskPart {
            ui8 VDiskIdx;
            ui8 PartIdx;

            TVDiskPart(ui8 vDiskIdx, ui8 partIdx)
                : VDiskIdx(vDiskIdx)
                , PartIdx(partIdx)
            {}

            TString ToString() const {
                TStringStream str;
                str << "{TVDiskPart VDiskIdx# " << (ui32)VDiskIdx;
                str << " PartIdx# " << (ui32)PartIdx;
                str << "}";
                return str.Str();
            }
        };

        TStackVec<TVDiskPart, 8> Records;

        TString ToString() const {
            TStringStream str;
            str << "{TPartPlacement Size# " << Records.size();
            str << "{";
            for (size_t i = 0; i < Records.size(); ++i) {
                str << "Records[" << i << "]# " << Records[i].ToString();
            }
            str << "}}";
            return str.Str();
        }
    };

    struct TResult {
        const ui32 PartCount = 0;
        const TString Error;

        TResult(ui32 partCount)
            : PartCount(partCount)
        {}

        TResult(const TString &error)
            : Error(error)
        {}

        bool Good() const {
            return Error.empty();
        }

        TString ToString() const {
            TStringStream str;
            if (Good()) {
                str << PartCount;
            } else {
                str << Error;
            }
            return str.Str();
        }
    };

    ui32 BlobSubgroupSize() const; // _4_+_2_+_1_
    ui32 Handoff() const; // 4 + 2 + _1_
    bool IsHandoffInSubgroup(ui32 idxInSubgroup) const; // True for idx of a handoff disk in blob subgoup

    bool CorrectLayout(const TPartLayout &layout, TPartPlacement &outCorrection) const;

    ui32 GetExpectedVGetReplyProtobufSize(const TLogoBlobID &id) const {
        return (BlobProtobufHeaderMaxSize + PartSize(id)) * TotalPartCount();
    }

    ui64 PartSize(const TLogoBlobID &id) const;
    ui64 MaxPartSize(const TLogoBlobID &id) const;
    ui64 MaxPartSize(TErasureType::ECrcMode crcMode, ui32 blobSize) const;

    bool PartFits(ui32 partId, ui32 idxInSubgroup) const;
};

}

