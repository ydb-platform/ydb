#pragma once
#include <ydb/library/actors/core/log.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/library/formats/arrow/protos/fields.pb.h>

#include <arrow/type.h>

namespace NKikimr::NArrow {

class TSchemaSubset {
private:
    std::vector<ui32> FieldIdx;
    TString FieldBits;
    bool Exclude = false;
public:
    TSchemaSubset() = default;
    TSchemaSubset(const std::set<ui32>& fieldsIdx, const ui32 fieldsCount);

    ui64 GetTxVolume() const {
        return FieldBits.size() + FieldIdx.size() * sizeof(ui32) + 1;
    }

    static TSchemaSubset AllFieldsAccepted() {
        TSchemaSubset result;
        result.Exclude = true;
        return result;
    }

    template <class TIterator>
    std::vector<typename std::iterator_traits<TIterator>::value_type> Apply(TIterator begin, TIterator end) const {
        using TValue = std::iterator_traits<TIterator>::value_type;
        if (FieldIdx.empty()) {
            return {std::move(begin), std::move(end)};
        }
        std::vector<TValue> fields;
        const ui64 size = end - begin;
        if (!Exclude) {
            for (auto&& i : FieldIdx) {
                AFL_VERIFY(i < size);
                fields.emplace_back(*(begin + i));
            }
        } else {
            auto it = FieldIdx.begin();
            for (ui32 i = 0; i < size; ++i) {
                if (it == FieldIdx.end() || i < *it) {
                    AFL_VERIFY(i < size);
                    fields.emplace_back(*(begin + i));
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