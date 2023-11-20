#pragma once
#include "defs.h"
#include <ydb/core/blobstorage/base/bufferwithgaps.h>
#include <ydb/core/blobstorage/vdisk/common/disk_part.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_blob.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <util/generic/deque.h>

namespace NKikimr {

    namespace NReadBatcher {

        ////////////////////////////////////////////////////////////////////////////////////
        // TGlueRead
        // This is ReadOp that goes to PDisk, it contains one or more elements we are going
        // to read from PDisk. We _glue_ these elements to avoid excessive seeks.
        ////////////////////////////////////////////////////////////////////////////////////
        struct TGlueRead {
            // part to read
            TDiskPart Part;
            // data we get from disk
            TBufferWithGaps Data;
            // was it read successfully?
            bool Success = false;

            TGlueRead(const TDiskPart &part)
                : Part(part)
                , Data()
            {}
        };

        struct TReadError {};

        using TGlueReads = TDeque<TGlueRead>;

        ////////////////////////////////////////////////////////////////////////////////////
        // User read request generates several items in response. TDataItem represents one
        // element
        ////////////////////////////////////////////////////////////////////////////////////
        class TDataItem {
        public:
            enum EType {
                ET_CLEAN,       // initial state
                ET_SETDISK,
                ET_SETMEM,
                ET_NODATA,
                ET_ERROR,
                ET_NOT_YET
            };
            EType Type;                 // type of this TDataItem
            TLogoBlobID Id;             // must be a concrete part
            void *Cookie;               // cookie provided by TReadBatcher user
            TIngress Ingress;           // final ingress of this logoblob
            ui32 GlueReqIdx;            // idx in GlueReads array
            // disk
            TDiskPart ActualRead;       // actual read from disk for this item
            // mem
            TRope MemData;              // found data in memory

            // extra found disk items for the case when ActualRead fails by disk corruption
            std::vector<TDiskPart> ExtraDiskItems;

        public:
            TString ToString() const {
                TStringStream str;
                str << "{Type# ";
                switch (Type) {
                    case ET_CLEAN:   str << "ET_CLEAN";   break;
                    case ET_SETDISK: str << "ET_SETDISK"; break;
                    case ET_SETMEM:  str << "ET_SETMEM";  break;
                    case ET_NODATA:  str << "ET_NODATA";  break;
                    case ET_ERROR:   str << "ET_ERROR";   break;
                    case ET_NOT_YET: str << "ET_NOT_YET"; break;
                }
                str << " Id# " << Id.ToString() << " ActualRead# " << ActualRead.ToString() << "}";
                return str.Str();
            }

            // def constructor
            TDataItem()
            {
                Clear();
            }

            // Clear
            void Clear() {
                Type = ET_CLEAN;
                Id = TLogoBlobID();
                Cookie = nullptr;
                Ingress = TIngress();
                GlueReqIdx = (ui32(-1));
                ActualRead.Clear();
                MemData = {};
                ExtraDiskItems.clear();
            }

            // Set NO_DATA
            void UpdateWithNoData(const TLogoBlobID &id, void *cookie) {
                Type = ET_NODATA;
                Id = id;
                Cookie = cookie;
            }

            // Set error
            void UpdateWithError(const TLogoBlobID &id, void *cookie) {
                Type = ET_ERROR;
                Id = id;
                Cookie = cookie;
            }

            // Set NOT_YET
            void UpdateWithNotYet(const TLogoBlobID &id, void *cookie) {
                Type = ET_NOT_YET;
                Id = id;
                Cookie = cookie;
            }

            // Update with disk data
            void UpdateWithDiskItem(const TLogoBlobID &id, void *cookie, const TDiskPart &actualRead) {
                if (Type == ET_SETDISK) { // add backup copy of data for the case when main reads as CORRUPTED
                    ExtraDiskItems.push_back(ActualRead);
                }

                Type = ET_SETDISK;
                Id = id;
                Cookie = cookie;
                ////// set disk
                ActualRead = actualRead;
            }

            // Update with mem data
            void UpdateWithMemItem(const TLogoBlobID &id, void *cookie, TRope data) {
                Type = ET_SETMEM;
                Id = id;
                Cookie = cookie;
                ////// clear disk
                ActualRead.Clear();
                ExtraDiskItems.clear();
                ////// set mem
                MemData = std::move(data);
            }

            void SetIngress(const TIngress &ingress) {
                Ingress = ingress;
            }

            bool Empty() const {
                return Type == ET_CLEAN;
            }

            bool ShouldUpdateWithDisk() const {
                Y_DEBUG_ABORT_UNLESS(Type == ET_CLEAN || Type == ET_SETDISK || Type == ET_SETMEM);
                return Type == ET_CLEAN || Type == ET_SETDISK;
            }

            bool ShouldUpdateWithMem() const {
                Y_DEBUG_ABORT_UNLESS(Type == ET_CLEAN || Type == ET_SETDISK || Type == ET_SETMEM);
                return Type == ET_CLEAN || Type == ET_SETDISK;
            }

            EType GetType() const {
                return Type;
            }

            bool ReadFromDisk() const {
                return Type == ET_SETDISK;
            }

            void SetGlueReqIdx(ui32 idx) {
                GlueReqIdx = idx;
            }

            template<typename TProcessor>
            void GetData(const TGlueReads &glueReads, TProcessor&& processor) const {
                Y_DEBUG_ABORT_UNLESS(Type == ET_SETDISK || Type == ET_SETMEM);

                if (Type == ET_SETMEM) {
                    processor(MemData);
                } else {
                    Y_DEBUG_ABORT_UNLESS(GlueReqIdx != (ui32)-1);
                    const TGlueRead &glue = glueReads[GlueReqIdx];

                    Y_DEBUG_ABORT_UNLESS(glue.Part.ChunkIdx == ActualRead.ChunkIdx &&
                                 glue.Part.Offset <= ActualRead.Offset &&
                                 (ActualRead.Offset + ActualRead.Size) <= (glue.Part.Offset + glue.Part.Size));

                    if (glue.Success && glue.Data.IsReadable(ActualRead.Offset - glue.Part.Offset, ActualRead.Size)) {
                        processor(glue.Data.Substr(ActualRead.Offset - glue.Part.Offset, ActualRead.Size));
                    } else {
                        processor(TReadError());
                    }
                }
            }

            static bool DiskPartLess(const TDataItem *x, const TDataItem *y) {
                Y_DEBUG_ABORT_UNLESS(x->ActualRead.Size && y->ActualRead.Size); // compare items with non-null Part only
                return x->ActualRead < y->ActualRead;
            }
        };

        using TDataItems = TDeque<TDataItem>;
        using TDiskDataItemPtrs = TDeque<TDataItem*>;

    } // NReadBatcher

    ////////////////////////////////////////////////////////////////////////////////////
    // TReadBatcherResult contains result of Read operation.
    // Firstly data items are being put to this structure.
    // Secondly they are being loaded into memory (those that are on disk).
    ////////////////////////////////////////////////////////////////////////////////////
    class TReadBatcherResult {
    public:
        // Data items that answer user query
        NReadBatcher::TDataItems DataItems;
        // Pointers to DataItems to load (they are pointers to DataItems stored in DataItems field)
        NReadBatcher::TDiskDataItemPtrs DiskDataItemPtrs;
        // Read ops to PDisk and their result
        NReadBatcher::TGlueReads GlueReads;

        class TIterator;
    };

    ////////////////////////////////////////////////////////////////////////////////////
    // TReadBatcherResult::TIterator
    // To iterate over results.
    ////////////////////////////////////////////////////////////////////////////////////
    class TReadBatcherResult::TIterator {
    public:
        TIterator(const TReadBatcherResult *rbr)
            : Rbr(rbr)
            , Cur(Rbr->DataItems.begin())
            , End(Rbr->DataItems.end())
        {}

        void SeekToFirst() {
            Cur = Rbr->DataItems.begin();
        }

        void Next() {
            ++Cur;
        }

        bool Valid() const {
            return Cur != End;
        }

        const NReadBatcher::TDataItem *Get() const {
            return &*Cur;
        }

        // FIXME: refactor
        template<typename TProcessor>
        void GetData(TProcessor&& processor) const {
            Cur->GetData(Rbr->GlueReads, std::forward<TProcessor>(processor));
        }

    private:
        const TReadBatcherResult *Rbr;
        NReadBatcher::TDataItems::const_iterator Cur;
        NReadBatcher::TDataItems::const_iterator End;
    };

    ////////////////////////////////////////////////////////////////////////////
    // TReadBatcherCtx
    ////////////////////////////////////////////////////////////////////////////
    struct TReadBatcherCtx : public TThrRefBase {
        TVDiskContextPtr VCtx;
        TPDiskCtxPtr PDiskCtx;
        TEvBlobStorage::TEvVGet::TPtr OrigEv;


        TReadBatcherCtx(TVDiskContextPtr vctx,
                        TPDiskCtxPtr pdiskCtx,
                        TEvBlobStorage::TEvVGet::TPtr &ev)
            : VCtx(std::move(vctx))
            , PDiskCtx(std::move(pdiskCtx))
            , OrigEv(ev)
        {}
    };

    typedef TIntrusivePtr<TReadBatcherCtx> TReadBatcherCtxPtr;

    ////////////////////////////////////////////////////////////////////////////
    // TReadBatcher
    ////////////////////////////////////////////////////////////////////////////
    class TReadBatcher {
    public:
        TReadBatcher(TReadBatcherCtxPtr ctx)
            : Ctx(ctx)
            , Result(std::make_shared<TReadBatcherResult>())
        {
            Y_DEBUG_ABORT_UNLESS(Ctx->VCtx->Top->GType.TotalPartCount() <= MaxTotalPartCount);
            TmpItems.resize(Ctx->VCtx->Top->GType.TotalPartCount());
        }

        // Traverse data parts for a single key
        void StartTraverse(const TLogoBlobID& id, void *cookie, ui8 queryPartId, ui32 queryShift, ui32 querySize);

        // NOTES about reading
        // We have the following possible combinations of shift and size
        // shift == 0 and size == 0   =>   read full blob
        // shift == 0 and size != 0   =>   read selected part
        // shift != 0 and size != 0   =>   read selected part
        // shift != 0 and size == 0   =>   read from shift and up to the end

        // We have data on disk
        void operator () (const TDiskPart &data, NMatrix::TVectorType parts);
        void ProcessFoundDiskItem(const TDiskPart &data, NMatrix::TVectorType parts);
        // We have diskBlob in memory
        void operator () (const TDiskBlob &diskBlob);
        void ProcessFoundInMemItem(const TDiskBlob &diskBlob);
        // Finish data traverse for a single key
        void FinishTraverse(const TIngress &ingress);
        // Abort traverse without giving out results
        void AbortTraverse();


        void PutNoData(const TLogoBlobID &id, const TMaybe<TIngress> &ingress, void *cookie) {
            NReadBatcher::TDataItem item;
            item.UpdateWithNoData(id, cookie);
            if (ingress) {
                item.SetIngress(*ingress);
            }
            Result->DataItems.push_back(item);
        }

        // creates an actor for efficient async data reads, returns nullptr
        // if no read required
        IActor *CreateAsyncDataReader(const TActorId &notifyID, ui8 priority, NWilson::TTraceId traceId, bool isRepl);

        const TReadBatcherResult &GetResult() const { return *Result; }
        ui64 GetPDiskReadBytes() const { return PDiskReadBytes; }

    private:
        TReadBatcherCtxPtr Ctx;
        // traversing
        TStackVec<NReadBatcher::TDataItem, MaxTotalPartCount> TmpItems;
        TLogoBlobID CurID;
        void *Cookie = nullptr;
        ui32 TraverseOffs = 0;
        bool Traversing = false;
        bool FoundAnything = false;
        ui8 QueryPartId = 0;
        ui32 QueryShift = 0;
        ui32 QuerySize = 0;
        // final result
        std::shared_ptr<TReadBatcherResult> Result;
        ui64 PDiskReadBytes = 0;

        TStackVec<std::tuple<TDiskPart, NMatrix::TVectorType>, MaxTotalPartCount> FoundDiskItems;
        TStackVec<TDiskBlob, MaxTotalPartCount> FoundInMemItems;

        NReadBatcher::TGlueRead *AddGlueRead(NReadBatcher::TDataItem *item);
        void PrepareReadPlan();
        TString DiskDataItemsToString() const;
        bool CheckDiskDataItemsOrdering(bool printOnFail = false) const;

        void ClearTmpItems() {
            for (auto &x : TmpItems)
                x.Clear();
        }
    };

} // NKikimr

