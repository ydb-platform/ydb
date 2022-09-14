#include "columnshard__costs.h"
#include <ydb/core/tx/columnshard/engines/index_info.h>
#include <ydb/core/tx/columnshard/engines/granules_table.h>
#include <ydb/core/formats/arrow_helpers.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/protos/ssa.pb.h>
#include <ydb/core/tx/columnshard/engines/predicate.h>

namespace NKikimr::NOlap::NCosts {

TKeyMark TCostsOperator::BuildMarkFromGranule(const TGranuleRecord& record) const {
    TKeyMark result;
    result.AddValue(std::make_shared<arrow::UInt64Scalar>(ExtractKey(record.IndexKey)));
    return result;
}

TRangeMark TCostsOperator::BuildMarkFromPredicate(const std::shared_ptr<NOlap::TPredicate>& p) const {
    if (!p) {
        return TRangeMark();
    }
    TRangeMark result;
    for (i32 i = 0; i < p->Batch->num_columns(); ++i) {
        auto column = p->Batch->column(i);
        Y_VERIFY(!!column && column->length() == 1);
        Y_VERIFY(!column->IsNull(0));
        auto status = column->GetScalar(0);
        Y_VERIFY(status.ok());
        result.MutableMark().AddValue(status.ValueUnsafe());
    }
    result.SetMarkIncluded(p->Inclusive);
    return result;
}

void TCostsOperator::FillRangeMarks(TKeyRanges& result, const std::shared_ptr<NOlap::TPredicate>& left, const std::shared_ptr<NOlap::TPredicate>& right) const {
    LOG_S_DEBUG("TCostsOperator::BuildRangeMarks::Request from " << (left ? left->Batch->ToString() : "-Inf") <<
        " to " << (right ? right->Batch->ToString() : "+Inf"));

    result.InitColumns(NArrow::MakeArrowSchema(IndexInfo.GetPK()));
    {
        TRangeMark leftBorder = BuildMarkFromPredicate(left);
        if (!leftBorder.GetMark().ColumnsCount()) {
            Y_VERIFY(!result.IsLeftBorderOpened());
            result.SetLeftBorderOpened(true);
        } else {
            Y_VERIFY(result.AddRangeIfNotLess(std::move(leftBorder)));
        }
    }
    for (auto&& i : GranuleRecords) {
        auto granuleMark = BuildMarkFromGranule(i);
        Y_VERIFY(result.AddRangeIfGrow(TRangeMark(std::move(granuleMark))));
    }
    {
        TRangeMark rightBorder = BuildMarkFromPredicate(right);
        if (rightBorder.GetMark().ColumnsCount()) {
            Y_VERIFY(result.AddRangeIfNotLess(std::move(rightBorder)));
            result.MutableRangeMarks().back().SetIntervalSkipped(true);
        }
    }
}

ui64 TCostsOperator::ExtractKey(const TString& key) {
    Y_VERIFY(key.size() == 8);
    return *reinterpret_cast<const ui64*>(key.data());
}

bool TRangeMark::operator<(const TRangeMark& item) const {
    return Mark < item.Mark;
}

TString TRangeMark::ToString() const {
    TStringBuilder sb;
    sb << "MARK=" << Mark.ToString() << ";SKIP=" << IntervalSkipped << ";";
    return sb;
}

NKikimrKqp::TEvRemoteCostData::TCostInfo TKeyRanges::SerializeToProto() const {
    NKikimrKqp::TEvRemoteCostData::TCostInfo result;
    result.SetLeftBorderOpened(LeftBorderOpened);
    for (auto&& i : RangeMarks) {
        *result.AddIntervalMeta() = i.SerializeMetaToProto();
    }

    std::shared_ptr<arrow::Schema> schema = KeyColumns;
    *result.MutableColumnsSchema() = NArrow::SerializeSchema(*schema);
    if (RangeMarks.empty()) {
        return result;
    }

    TVector<std::unique_ptr<arrow::ArrayBuilder>> arrayBuilders;
    for (auto&& f : KeyColumns->fields()) {
        std::unique_ptr<arrow::ArrayBuilder> arrayBuilder;
        Y_VERIFY(arrow::MakeBuilder(arrow::default_memory_pool(), f->type(), &arrayBuilder).ok());
        arrayBuilders.emplace_back(std::move(arrayBuilder));
        Y_VERIFY(arrayBuilders.back()->Reserve(RangeMarks.size()).ok());
    }
    for (auto&& i : RangeMarks) {
        Y_VERIFY(i.GetMark().ColumnsCount() <= ColumnsCount());
        auto itBuilder = arrayBuilders.begin();
        for (auto&& scalar : i.GetMark()) {
            Y_VERIFY(!!scalar);
            auto correctScalar = scalar->CastTo((*itBuilder)->type());
            Y_VERIFY(correctScalar.ok());
            Y_VERIFY((*itBuilder)->AppendScalar(**correctScalar).ok());
            ++itBuilder;
        }
        for (; itBuilder != arrayBuilders.end(); ++itBuilder) {
            Y_VERIFY((*itBuilder)->AppendNull().ok());
        }
    }
    TVector<std::shared_ptr<arrow::Array>> arrays;
    for (auto&& i : arrayBuilders) {
        arrow::Result<std::shared_ptr<arrow::Array>> aData = i->Finish();
        Y_VERIFY(aData.ok());
        arrays.emplace_back(aData.ValueUnsafe());
    }
    std::shared_ptr<arrow::RecordBatch> rb = arrow::RecordBatch::Make(schema, RangeMarks.size(), arrays);
    *result.MutableColumnsData() = NArrow::SerializeBatchNoCompression(rb);
    return result;
}

bool TKeyRanges::DeserializeFromProto(const NKikimrKqp::TEvRemoteCostData::TCostInfo& proto) {
    std::shared_ptr<arrow::Schema> schema = NArrow::DeserializeSchema(proto.GetColumnsSchema());
    Y_VERIFY(schema);
    LeftBorderOpened = proto.GetLeftBorderOpened();
    KeyColumns = schema;

    TVector<TRangeMark> resultLocal;
    if (!!proto.GetColumnsData()) {
        std::shared_ptr<arrow::RecordBatch> batch = NArrow::DeserializeBatch(proto.GetColumnsData(), schema);
        Y_VERIFY(batch->num_columns() == (int)ColumnsCount());
        resultLocal.reserve(batch->num_rows());
        for (ui32 rowIdx = 0; rowIdx < batch->num_rows(); ++rowIdx) {
            TKeyMark mark;
            for (ui32 cIdx = 0; cIdx < ColumnsCount(); ++cIdx) {
                if (batch->column(cIdx)->IsNull(rowIdx)) {
                    break;
                }
                auto valueStatus = batch->column(cIdx)->GetScalar(rowIdx);
                Y_VERIFY(valueStatus.ok());
                mark.AddValue(valueStatus.ValueUnsafe());
            }
            resultLocal.emplace_back(TRangeMark(std::move(mark)));
        }
    }
    Y_VERIFY(resultLocal.size() == (size_t)proto.GetIntervalMeta().size());
    ui32 idx = 0;
    for (auto&& i : proto.GetIntervalMeta()) {
        if (!resultLocal[idx++].DeserializeMetaFromProto(i)) {
            return false;
        }
    }
    std::swap(RangeMarks, resultLocal);
    return true;
}

bool TKeyRanges::AddRangeIfGrow(TRangeMark&& range) {
    Y_VERIFY(range.GetMark().ColumnsCount() <= ColumnsCount());
    if (RangeMarks.empty() || RangeMarks.back() < range) {
        RangeMarks.emplace_back(std::move(range));
        return true;
    }
    return false;
}

bool TKeyRanges::AddRangeIfNotLess(TRangeMark&& range) {
    Y_VERIFY(range.GetMark().ColumnsCount() <= ColumnsCount());
    if (RangeMarks.empty() || RangeMarks.back() < range || !(range < RangeMarks.back())) {
        RangeMarks.emplace_back(std::move(range));
        return true;
    }
    return RangeMarks.back() < range;
}

TString TKeyRanges::ToString() const {
    TStringBuilder sb;
    sb << "COLUMNS: " << KeyColumns->ToString() << Endl;
    sb << Endl << "DATA: " << Endl;
    for (auto&& i : RangeMarks) {
        sb << i.ToString() << ";";
    }
    return sb;
}

TKeyRanges::TKeyRanges() {
    KeyColumns = arrow::SchemaBuilder().Finish().ValueUnsafe();
}

TString TKeyMark::ToString() const {
    TStringBuilder sb;
    for (auto&& i : Values) {
        sb << (i ? i->ToString() : "NO_VALUE") << ";";
    }
    return sb;
}

bool TKeyMark::operator<(const TKeyMark& item) const {
    auto itSelf = Values.begin();
    auto itItem = item.Values.begin();
    for (; itSelf != Values.end() && itItem != item.Values.end(); ++itSelf, ++itItem) {
        if (NArrow::ScalarLess(*itSelf, *itItem)) {
            return true;
        } else if (NArrow::ScalarLess(*itItem, *itSelf)) {
            return false;
        }
    }
    if (itSelf == Values.end()) {
        if (itItem == item.Values.end()) {
            return false;
        } else {
            return true;
        }
    } else {
        if (itItem == item.Values.end()) {
            return true;
        } else {
            Y_VERIFY(false, "impossible");
        }
    }
    return false;
}

}
