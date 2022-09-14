#include "kqp_compute.h"

namespace NKikimr::NKqp {

void TEvKqpCompute::TEvCostData::InitRemote() const {
    if (!Remote) {
        Remote.Reset(new TEvRemoteCostData);
        *Remote->Record.MutableCostInfo() = TableRanges.SerializeToProto();
        Remote->Record.SetScanId(ScanId);
    }
}

NActors::IEventBase* TEvKqpCompute::TEvCostData::Load(TEventSerializedData* data) {
    auto pbEv = THolder<TEvRemoteCostData>(static_cast<TEvRemoteCostData*>(TEvRemoteCostData::Load(data)));
    NOlap::NCosts::TKeyRanges ranges;
    Y_VERIFY(ranges.DeserializeFromProto(pbEv->Record.GetCostInfo()));
    THolder<TEvCostData> result(new TEvCostData(std::move(ranges), pbEv->Record.GetScanId()));
    return result.Release();
}

TVector<NKikimr::TSerializedTableRange> TEvKqpCompute::TEvCostData::GetSerializedTableRanges() const {
    TVector<TSerializedTableRange> result;
    TVector<TCell> borderPred;
    borderPred.resize(TableRanges.ColumnsCount(), TCell());
    bool predSkipped = !TableRanges.IsLeftBorderOpened();
    bool predIncluded = false;
    for (auto&& i : TableRanges.GetRangeMarks()) {
        TVector<TCell> borderCurrent;
        for (auto&& value : i.GetMark()) {
            borderCurrent.emplace_back(SerializeScalarToCell(value));
        }
        if (!predSkipped) {
            for (ui32 additional = borderPred.size(); additional < TableRanges.ColumnsCount(); ++additional) {
                borderPred.emplace_back(TCell());
            }
            if (!i.GetMarkIncluded()) {
                for (ui32 additional = borderCurrent.size(); additional < TableRanges.ColumnsCount(); ++additional) {
                    borderCurrent.emplace_back(TCell());
                }
            }
            TSerializedTableRange serializedRange(borderPred, predIncluded, borderCurrent, i.GetMarkIncluded());
            result.emplace_back(std::move(serializedRange));
        }
        predSkipped = i.GetIntervalSkipped();
        predIncluded = i.GetMarkIncluded();
        std::swap(borderCurrent, borderPred);
    }
    if (predSkipped) {
        Y_VERIFY(result.size());
        result.back().ToInclusive = predIncluded;
    } else {
        TSerializedTableRange serializedRange(borderPred, predIncluded, TVector<TCell>(), false);
        result.emplace_back(std::move(serializedRange));
    }
    return result;
}

NKikimr::TCell TEvKqpCompute::TEvCostData::SerializeScalarToCell(const std::shared_ptr<arrow::Scalar>& x) const {
    if (!x) {
        return TCell();
    }
    auto castStatus = std::dynamic_pointer_cast<arrow::BaseBinaryScalar>(x);
    if (castStatus) {
        TCell resultCell((char*)castStatus->value->data(), castStatus->value->size());
        return resultCell;
    } else {
        TCell resultCell;
        Y_VERIFY(NArrow::SwitchType(x->type->id(), [&](const auto& type) {
            using TWrap = std::decay_t<decltype(type)>;
            using TScalar = typename arrow::TypeTraits<typename TWrap::T>::ScalarType;
            using TValue = std::decay_t<decltype(static_cast<const TScalar&>(*x).value)>;

            if (!std::is_arithmetic_v<TValue>) {
                return false;
            }
            resultCell = TCell::Make(static_cast<const TScalar&>(*x).value);
            return true;
            }));
        return resultCell;
    }
}

}
