#pragma once

#include "yql_mkql_input_stream.h"

#include <ydb/library/yql/providers/common/codec/yql_codec.h>
#include <ydb/library/yql/providers/yt/codec/yt_codec_io.h>
#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/minikql/mkql_node.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/ptr.h>


namespace NYql {

class TFileInputState: public IInputState {
public:
    TFileInputState(const TMkqlIOSpecs& spec,
        const NKikimr::NMiniKQL::THolderFactory& holderFactory,
        TVector<NYT::TRawTableReaderPtr>&& rawReaders,
        size_t blockCount,
        size_t blockSize);

    size_t GetTableIndex() const {
        return CurrentInput_;
    }

    size_t GetRecordIndex() const {
        return CurrentRecord_; // returns 1-based index
    }

    void SetNextBlockCallback(std::function<void()> cb) {
        MkqlReader_.SetNextBlockCallback(cb);
        OnNextBlockCallback_ = std::move(cb);
    }

protected:
    virtual bool IsValid() const override {
        return Valid_;
    }

    virtual NUdf::TUnboxedValue GetCurrent() override {
        return CurrentValue_;
    }

    virtual void Next() override {
        Valid_ = NextValue();
    }

    void Finish() {
        MkqlReader_.Finish();
    }

    bool NextValue();

private:
    const TMkqlIOSpecs* Spec_;
    const NKikimr::NMiniKQL::THolderFactory& HolderFactory_;
    TVector<NYT::TRawTableReaderPtr> RawReaders_;
    const size_t BlockCount_;
    const size_t BlockSize_;
    size_t CurrentInput_ = 0;
    size_t CurrentRecord_ = 0;
    bool Valid_ = true;
    NUdf::TUnboxedValue CurrentValue_;
    NYT::TRawTableReaderPtr CurrentReader_;
    TMkqlReaderImpl MkqlReader_;  // Should be deleted before CurrentReader_
    std::function<void()> OnNextBlockCallback_;
};

TVector<NYT::TRawTableReaderPtr> MakeMkqlFileInputs(const TVector<TString>& files, bool decompress);

} // NYql
