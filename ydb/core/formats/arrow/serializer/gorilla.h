#pragma once

#include "abstract.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/conclusion/result.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/options.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NArrow::NSerialization {

    class TGorillaSerializer: public ISerializer {
    public:
        static TString GetClassNameStatic() {
            return "ARROW_SERIALIZER_GORILLA";
        }
    private:
        static const inline TFactory::TRegistrator<TGorillaSerializer> Registrator = TFactory::TRegistrator<TGorillaSerializer>(GetClassNameStatic());
    protected:
        uint64_t getU64FromArrayData(
                std::shared_ptr<arrow::DataType> &column_type,
                std::shared_ptr<arrow::ArrayData> &array_data,
                size_t i
        ) const;
        std::shared_ptr<arrow::ArrayBuilder> getColumnBuilderByType(
                std::shared_ptr<arrow::DataType> &column_type
        ) const;
        arrow::Status builderAppendValue(
                std::shared_ptr<arrow::DataType> &column_type,
                std::shared_ptr<arrow::ArrayBuilder> &column_builder,
                uint64_t value
        ) const;
        std::vector<uint64_t> getU64VecFromBatch(
                const std::shared_ptr<arrow::RecordBatch> &batch,
                size_t column_index
        ) const;
        template<typename T, typename F>
        arrow::Result<TString> serializeBatchEntities(
                const std::shared_ptr<arrow::Schema> &batch_schema,
                std::vector<T> &entities,
                F create_c_func
        ) const;

        virtual bool IsCompatibleForExchangeWithSameClass(const ISerializer& /*item*/) const override {
            return true;
        }

        virtual bool IsEqualToSameClass(const ISerializer& item) const override {
            Y_UNUSED(item);
            return true;
        }
        virtual TString DoSerializeFull(const std::shared_ptr<arrow::RecordBatch>& batch) const override;
        virtual TString DoSerializePayload(const std::shared_ptr<arrow::RecordBatch>& batch) const override;
        virtual arrow::Result<std::shared_ptr<arrow::RecordBatch>> DoDeserialize(const TString& data) const override;
        virtual arrow::Result<std::shared_ptr<arrow::RecordBatch>> DoDeserialize(const TString& data, const std::shared_ptr<arrow::Schema>& schema) const override;

        virtual TConclusionStatus DoDeserializeFromRequest(NYql::TFeaturesExtractor& features) override;

        virtual TConclusionStatus DoDeserializeFromProto(const NKikimrSchemeOp::TOlapColumn::TSerializer& proto) override;

        virtual void DoSerializeToProto(NKikimrSchemeOp::TOlapColumn::TSerializer& proto) const override;

    public:
        virtual TString GetClassName() const override {
            return GetClassNameStatic();
        }

        virtual bool IsHardPacker() const override {
            return false;
        }

        TGorillaSerializer() {}
    };
}