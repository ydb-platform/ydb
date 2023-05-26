#include "column_engine.h"
#include <util/stream/output.h>

namespace NKikimr::NOlap {

TString TMark::SerializeScalar(const NArrow::TReplaceKey& key, const std::shared_ptr<arrow::Schema>& schema) {
    Y_VERIFY(key.Size() == 1);
    Y_VERIFY_S(key.Column(0).type()->Equals(schema->field(0)->type()),
        key.Column(0).type()->ToString() + ", expected " + schema->ToString());
    return SerializeKeyScalar(NArrow::TReplaceKey::ToScalar(key));
}

NArrow::TReplaceKey TMark::DeserializeScalar(const TString& key, const std::shared_ptr<arrow::Schema>& schema) {
    Y_VERIFY(schema->num_fields() == 1);
    return NArrow::TReplaceKey::FromScalar(DeserializeKeyScalar(key, schema->field(0)->type()));
}

TString TMark::SerializeComposite(const NArrow::TReplaceKey& key, const std::shared_ptr<arrow::Schema>& schema) {
    auto batch = key.ToBatch(schema);
    Y_VERIFY(batch && batch->num_rows() == 1);
    return NArrow::SerializeBatchNoCompression(batch);
}

NArrow::TReplaceKey TMark::DeserializeComposite(const TString& key, const std::shared_ptr<arrow::Schema>& schema) {
    auto batch = NArrow::DeserializeBatch(key, schema);
    Y_VERIFY(batch && batch->num_rows() == 1);
    return NArrow::TReplaceKey::FromBatch(batch, 0);
}

std::string TMark::ToString() const {
    if (Border.Size() == 1) {
        return NArrow::TReplaceKey::ToScalar(Border)->ToString();
    } else {
        TStringBuilder out;
        out << "(";
        for (int i = 0; i < Border.Size(); ++i) {
            if (i) {
                out << ", ";
            }
            out << NArrow::TReplaceKey::ToScalar(Border, i)->ToString();
        }
        out << ")";
        return out;
    }
}

std::shared_ptr<arrow::Scalar> TMark::MinScalar(const std::shared_ptr<arrow::DataType>& type) {
    if (type->id() == arrow::Type::TIMESTAMP) {
        // TODO: support negative timestamps in index
        return std::make_shared<arrow::TimestampScalar>(0, type);
    }
    return NArrow::MinScalar(type);
}

NArrow::TReplaceKey TMark::MinBorder(const std::shared_ptr<arrow::Schema>& schema) {
    if (schema->num_fields() == 1) {
        return NArrow::TReplaceKey::FromScalar(MinScalar(schema->field(0)->type()));
    } else {
        std::vector<std::shared_ptr<arrow::Array>> columns;
        columns.reserve(schema->num_fields());
        for (const auto& field : schema->fields()) {
            auto scalar = MinScalar(field->type());
            Y_VERIFY_DEBUG(scalar);
            auto res = arrow::MakeArrayFromScalar(*scalar, 1);
            Y_VERIFY_DEBUG(res.ok());
            columns.emplace_back(*res);
        }
        return NArrow::TReplaceKey::FromBatch(arrow::RecordBatch::Make(schema, 1, columns), 0);
    }
}

}

template <>
void Out<NKikimr::NOlap::TColumnEngineChanges>(IOutputStream& out, TTypeTraits<NKikimr::NOlap::TColumnEngineChanges>::TFuncParam changes) {
    if (ui32 switched = changes.SwitchedPortions.size()) {
        out << "switch " << switched << " portions";
        for (auto& portionInfo : changes.SwitchedPortions) {
            out << portionInfo;
        }
        out << "; ";
    }
    if (ui32 added = changes.AppendedPortions.size()) {
        out << "add " << added << " portions";
        for (auto& portionInfo : changes.AppendedPortions) {
            out << portionInfo;
        }
        out << "; ";
    }
    if (ui32 moved = changes.PortionsToMove.size()) {
        out << "move " << moved << " portions";
        for (auto& [portionInfo, granule] : changes.PortionsToMove) {
            out << portionInfo << " (to " << granule << ")";
        }
        out << "; ";
    }
    if (ui32 evicted = changes.PortionsToEvict.size()) {
        out << "evict " << evicted << " portions";
        for (auto& [portionInfo, evictionFeatures] : changes.PortionsToEvict) {
            out << portionInfo << " (to " << evictionFeatures.TargetTierName << ")";
        }
        out << "; ";
    }
    if (ui32 dropped = changes.PortionsToDrop.size()) {
        out << "drop " << dropped << " portions";
        for (auto& portionInfo : changes.PortionsToDrop) {
            out << portionInfo;
        }
    }
}
