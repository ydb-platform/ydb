#pragma once
#include <ydb/core/formats/arrow/replace_key.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NArrow {

class TSpecialKeys {
protected:
    std::shared_ptr<arrow::RecordBatch> Data;

    bool DeserializeFromString(const TString& data);

    std::optional<TReplaceKey> GetKeyByIndex(const ui32 position, const std::shared_ptr<arrow::Schema>& schema) const;

    TSpecialKeys() = default;
    TSpecialKeys(std::shared_ptr<arrow::RecordBatch> data)
        : Data(data) {
        Y_VERIFY(Data);
        Y_VERIFY(Data->num_rows());
    }

public:

    TSpecialKeys(const TString& data) {
        Y_VERIFY(DeserializeFromString(data));
    }

    TString SerializeToString() const;
};

class TFirstLastSpecialKeys: public TSpecialKeys {
private:
    using TBase = TSpecialKeys;
public:
    std::optional<TReplaceKey> GetMin(const std::shared_ptr<arrow::Schema>& schema) const {
        return GetKeyByIndex(0, schema);
    }
    std::optional<TReplaceKey> GetMax(const std::shared_ptr<arrow::Schema>& schema) const {
        return GetKeyByIndex(Data->num_rows() - 1, schema);
    }

    explicit TFirstLastSpecialKeys(const TString& data);

    explicit TFirstLastSpecialKeys(std::shared_ptr<arrow::RecordBatch> batch, const std::vector<TString>& columnNames = {});
};

}
