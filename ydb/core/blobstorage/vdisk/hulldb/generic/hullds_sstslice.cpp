#include "hullds_sstslice_it.h"
#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr {

    template <class TKey, class TMemRec>
    void TLevelSlice<TKey, TMemRec>::OutputHtml(IOutputStream &str) const {
        TIdxDiskPlaceHolder::TInfo sum;
        HTML(str) {
            TABLE_CLASS ("table table-condensed") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() {str << "&#8470;";} // numero sign
                        TABLEH() {str << "Level";}
                        TABLEH() {str << "First / Last Lsn";}
                        TABLEH() {str << "Idx / Inplaced / Huge Size";}
                        TABLEH() {str << "Chunks / Index";}
                        TABLEH() {str << "Items / WInplData / WHugeData";}
                        TABLEH() {str << "First / Last Key";}
                        TABLEH() {str << NHullComp::TSstRatio::MonHeader();}
                        TABLEH() {str << "Origin";}
                        TABLEH() {str << "CTime";}
                    }
                }
                TABLEBODY() {
                    ui32 index = 1;

                    if (!Level0.Empty()) {
                        Level0.OutputHtml(index, str, sum);
                    }

                    ui32 level = 1;
                    for (const auto &x : SortedLevels) {
                        if (!x.Empty()) {
                            x.OutputHtml(index, level, str, sum);
                        }
                        ++level;
                    }
                    // total
                    TABLER() {
                        TABLED() {SMALL() {str << "index";}}
                        TABLED() {SMALL() {str << "total";}}
                        TABLED() {SMALL() {str << "lsns";}}
                        TABLED() {SMALL() {str << sum.IdxTotalSize << " / " << sum.InplaceDataTotalSize << " / "
                                           << sum.HugeDataTotalSize;}}
                        TABLED() {SMALL() {str << sum.Chunks << " / " << sum.IndexParts;}}
                        TABLED() {SMALL() {str << sum.Items << " / " << sum.ItemsWithInplacedData << " / "
                                           << sum.ItemsWithHugeData;}}
                        TABLED() {SMALL() {str << "keys";}}
                        TABLED() {SMALL() {str << "ratio";}}
                        TABLED() {SMALL() {str << "time";}}
                    }
                }
            }
        }
    }


    template <class TKey, class TMemRec>
    void TLevelSlice<TKey, TMemRec>::OutputProto(google::protobuf::RepeatedPtrField<NKikimrVDisk::LevelStat> *rows) const {

        if (!Level0.Empty()) {
            Level0.OutputProto(rows);
        }
        ui32 level = 1;
        for (const auto &x : SortedLevels) {
            if (!x.Empty()) {
                x.OutputProto(level, rows);
            }
            ++level;
        }
    }

    template struct TLevelSlice<TKeyLogoBlob, TMemRecLogoBlob>;
    template struct TLevelSlice<TKeyBarrier, TMemRecBarrier>;
    template struct TLevelSlice<TKeyBlock, TMemRecBlock>;

    template class TLevelSliceSnapshot<TKeyLogoBlob, TMemRecLogoBlob>;
    template class TLevelSliceSnapshot<TKeyBarrier, TMemRecBarrier>;
    template class TLevelSliceSnapshot<TKeyBlock, TMemRecBlock>;

} // NKikimr
