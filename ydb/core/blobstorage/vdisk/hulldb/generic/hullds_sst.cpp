#include "hullds_sst.h"
#include "hullds_sst_it.h"
#include <ydb/core/blobstorage/base/utility.h>
#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr {

    template <class TKey, class TMemRec>
    void TLevelSegment<TKey, TMemRec>::OutputHtml(ui32 &index, ui32 level, IOutputStream &str, TIdxDiskPlaceHolder::TInfo &sum) const {
        HTML(str) {
                if (IsLoaded()) {
                    TABLER() {
                        TABLED() {SMALL() {str << index;}}
                        TABLED() {SMALL() {str << level;}}
                        TABLED() {SMALL() {str << Info.FirstLsn << " / " << Info.LastLsn;}}
                        TABLED() {SMALL() {str << Info.IdxTotalSize << " / " << Info.InplaceDataTotalSize << " / "
                                << Info.HugeDataTotalSize;}}
                        TABLED() {SMALL() {str << Info.Chunks << " / " << Info.IndexParts;}}
                        TABLED() {SMALL() {str << Info.Items << " / " << Info.ItemsWithInplacedData << " / "
                                << Info.ItemsWithHugeData;}}
                        TABLED() {SMALL() {str << FirstKey().ToString() << "\n" << LastKey().ToString();}}
                        TABLED() {SMALL() {str << StorageRatio.MonSummary();}}
                        TABLED() {SMALL() {str << (Info.IsCreatedByRepl() ? "REPL" : "COMP");}}
                        TABLED() {SMALL() {str << ToStringLocalTimeUpToSeconds(Info.CTime);}}
                        ++index;
                    }
                }
        }
        sum += Info;
    }

    template <class TKey, class TMemRec>
    void TLevelSegment<TKey, TMemRec>::OutputProto(ui32 level, google::protobuf::RepeatedPtrField<NKikimrVDisk::LevelStat> *rows) const {
        if (IsLoaded()) {
            NKikimrVDisk::LevelStat *row = rows->Add();
            row->set_level(level);
            row->set_first_lsn(Info.FirstLsn);
            row->set_last_lsn(Info.LastLsn);
            row->set_idx_total_size(Info.IdxTotalSize);
            row->set_inplace_data_total_size(Info.InplaceDataTotalSize);
            row->set_huge_data_total_size(Info.HugeDataTotalSize);
            row->set_chunks(Info.Chunks);
            row->set_total_chunks(Info.IndexParts);
            row->set_items(Info.Items);
            row->set_items_with_inplaced_data(Info.ItemsWithInplacedData);
            row->set_items_with_huge_data(Info.ItemsWithHugeData);
            row->set_first_key(FirstKey().ToString());
            row->set_last_key(LastKey().ToString());
            StorageRatio.OutputProto(row->mutable_storage_ratio());
            row->set_is_created_by_repl(Info.IsCreatedByRepl());
            row->set_time(ToStringLocalTimeUpToSeconds(Info.CTime));
        }
    }

    template <class TKey, class TMemRec>
    void TLevelSegment<TKey, TMemRec>::Output(IOutputStream &str) const {
        str << "[" << FirstKey().ToString() << " " << LastKey().ToString() << " Info# ";
        Info.Output(str);
        str << " Ratio# " << StorageRatio.MonSummary() << "]";
    }

    template struct TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob>;
    template struct TLevelSegment<TKeyBarrier, TMemRecBarrier>;
    template struct TLevelSegment<TKeyBlock, TMemRecBlock>;

} // NKikimr
