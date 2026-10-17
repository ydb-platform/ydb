#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_signature.h>
#include <ydb/core/blobstorage/vdisk/common/disk_part.h>

#include <util/stream/str.h>

namespace NKikimr {

    template <class TKey>
    TLogSignature PDiskSignatureForHullDbKey();

    ///////////////////////////////////////////////////////////////////////////////////////
    // TDiskDataExtractor
    ///////////////////////////////////////////////////////////////////////////////////////
    struct TDiskDataExtractor {
        TBlobType::EType BlobType = TBlobType::DiskBlob;
        const TDiskPart *Begin = nullptr;
        const TDiskPart *End = nullptr;
        TDiskPart Store;

        // set up one part (DiskBlob or HugeBlob)
        void Set(TBlobType::EType t, const TDiskPart &part) {
            BlobType = t;
            Store = part;
            Begin = &Store;
            End = Begin + 1;
        }

        // set up many parts (ManyHugeBlobs)
        void Set(TBlobType::EType t, const TDiskPart *begin, const TDiskPart *end) {
            BlobType = t;
            Begin = begin;
            End = end;
            Store.Clear();
        }

        void Clear() {
            BlobType = TBlobType::DiskBlob;
            Begin = End = nullptr;
            Store.Clear();
        }

        const TDiskPart &SwearOne() const {
            Y_DEBUG_ABORT_UNLESS(Begin + 1 == End);
            return *Begin;
        }

        TString ToString() const {
            if (Begin == End)
                return "empty";
            else {
                TStringStream str;
                bool first = true;
                for (const TDiskPart *it = Begin; it != End; ++it) {
                    if (first)
                        first = false;
                    else
                        str << " ";
                    str << it->ToString();
                }
                return str.Str();
            }
        }


        ui32 GetInplacedDataSize() const {
            if (BlobType == TBlobType::DiskBlob && Begin) {
                return Begin->Size;
            } else {
                return 0;
            }
        }

        ui32 GetHugeDataSize() const {
            if (BlobType == TBlobType::HugeBlob || BlobType == TBlobType::ManyHugeBlobs) {
                ui32 size = 0;
                for (const TDiskPart *cur = Begin; cur != End; cur++) {
                    size += cur->Size;
                }
                if (End - Begin > 1) {
                    // add outbound expenses
                    size += (End - Begin) * sizeof(TDiskPart);
                }
                return size;
            } else {
                return 0;
            }
        }
    };

    ///////////////////////////////////////////////////////////////////////////////////////
    // TMemPart
    ///////////////////////////////////////////////////////////////////////////////////////
    struct TMemPart {
        ui64 BufferId;
        ui32 Size;

        TMemPart(ui64 bufferId, ui32 size)
            : BufferId(bufferId)
            , Size(size)
        {}
    };

} // NKikimr
