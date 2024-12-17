#pragma once
#include <ydb/library/formats/arrow/transformer/abstract.h>

namespace NKikimr::NArrow::NTransformation {

class TDictionaryPackTransformer: public ITransformer {
public:
    static TString GetClassNameStatic() {
        return "DICT_PACK";
    }
protected:
    virtual std::shared_ptr<arrow::RecordBatch> DoTransform(const std::shared_ptr<arrow::RecordBatch>& batch) const override;
    virtual TString DoDebugString() const override {
        return "type=DICT_PACK;";
    }
    virtual bool IsEqualToSameClass(const ITransformer& /*item*/) const override {
        return true;
    }
public:
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

class TDictionaryUnpackTransformer: public ITransformer {
public:
    static TString GetClassNameStatic() {
        return "DICT_UNPACK";
    }
protected:
    virtual std::shared_ptr<arrow::RecordBatch> DoTransform(const std::shared_ptr<arrow::RecordBatch>& batch) const override;
    virtual TString DoDebugString() const override {
        return "type=DICT_UNPACK;";
    }
    virtual bool IsEqualToSameClass(const ITransformer& /*item*/) const override {
        return true;
    }
public:
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}
