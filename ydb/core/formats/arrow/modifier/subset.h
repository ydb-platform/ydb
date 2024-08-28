#pragma once
#include <ydb/core/formats/arrow/protos/fields.pb.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NArrow {

class TSchemaSubset {
private:
    std::vector<ui32> FieldIdx;
    TString FieldBits;
    bool Exclude = false;
public:
    TSchemaSubset() = default;
    TSchemaSubset(const std::set<ui32>& fieldsIdx, const ui32 fieldsCount);

    static TSchemaSubset AllFieldsAccepted() {
        TSchemaSubset result;
        result.Exclude = true;
        return result;
    }

    template <class T>
    std::vector<T> Apply(const std::vector<T>& fullSchema) const {
        if (FieldIdx.empty()) {
            return fullSchema;
        }
        std::vector<T> fields;
        if (!Exclude) {
            for (auto&& i : FieldIdx) {
                AFL_VERIFY(i < fullSchema.size());
                fields.emplace_back(fullSchema[i]);
            }
        } else {
            auto it = FieldIdx.begin();
            for (ui32 i = 0; i < fullSchema.size(); ++i) {
                if (it == FieldIdx.end() || i < *it) {
                    AFL_VERIFY(i < fullSchema.size());
                    fields.emplace_back(fullSchema[i]);
                } else if (i == *it) {
                    ++it;
                } else {
                    AFL_VERIFY(false);
                }
            }
        }
        return fields;
    }

    NKikimrArrowSchema::TSchemaSubset SerializeToProto() const;
    [[nodiscard]] TConclusionStatus DeserializeFromProto(const NKikimrArrowSchema::TSchemaSubset& proto);
};

}