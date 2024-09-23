#pragma once

#include "yt_codec.h"
#include <ydb/library/yql/providers/common/codec/yql_codec_buf.h>

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/aligned_page_pool.h>

#ifndef MKQL_DISABLE_CODEGEN
#include <ydb/library/yql/minikql/codegen/codegen.h>
#endif

#include <yt/cpp/mapreduce/interface/io.h>
#include <yt/cpp/mapreduce/io/stream_table_reader.h>

#include <library/cpp/yson/zigzag.h>

#include <util/generic/strbuf.h>
#include <util/generic/ptr.h>
#include <util/generic/xrange.h>
#include <util/generic/yexception.h>
#include <util/generic/maybe.h>
#include <util/stream/output.h>
#include <util/stream/input.h>
#include <util/stream/holder.h>
#include <util/system/compiler.h>

namespace NYql {

//////////////////////////////////////////////////////////////////////////////////////////////////////////

class TMkqlReaderImpl : public IMkqlReaderImpl {
public:
    struct TDecoder;

    // Input source must be valid while TMkqlReaderImpl exists
    TMkqlReaderImpl();
    TMkqlReaderImpl(NYT::TRawTableReader& source, size_t blockCount, size_t blockSize, ui32 tableIndex = 0, bool ignoreStreamTableIndex = false);
    ~TMkqlReaderImpl();

    void SetReader(NYT::TRawTableReader& source, size_t blockCount, size_t blockSize, ui32 tableIndex = 0, bool ignoreStreamTableIndex = false, TMaybe<ui64> currentRow={}, TMaybe<ui32> currentRange={});
    void SetSpecs(const TMkqlIOSpecs& specs, const NKikimr::NMiniKQL::THolderFactory& holderFactory);
    void SetDeadline(TInstant deadline) {
        Reader_->SetDeadline(deadline);
    }
    void SetNextBlockCallback(std::function<void()> cb) {
        Buf_.SetNextBlockCallback(cb);
    }

    NKikimr::NUdf::TUnboxedValue GetRow() const override;
    bool IsValid() const override;
    void Next() override;
    TMaybe<ui32> GetRangeIndexUnchecked() const;
    TMaybe<ui64> GetRowIndexUnchecked() const;
    ui32 GetTableIndex() const override;
    ui32 GetRangeIndex() const override;
    ui64 GetRowIndex() const override;
    void NextKey() override;

    void Finish();

protected:
    NKikimr::NUdf::TUnboxedValue BuildOthers(const TMkqlIOSpecs::TDecoderSpec& decoder, NKikimr::NMiniKQL::TValuesDictHashMap& others);

    void CheckValidity() const;
    void CheckReadRow() const;

    void OnError(const std::exception_ptr& error, TStringBuf msg);

protected:
    NKikimr::NMiniKQL::TSamplingStatTimer TimerDecode_;
    NKikimr::NMiniKQL::TSamplingStatTimer TimerRead_;
    THolder<NCommon::IBlockReader> Reader_;
    NCommon::TInputBuf Buf_;
    THolder<TDecoder> Decoder_;

    bool HasRangeIndices_ = false;
    ui32 InitialTableIndex_ = 0;
    bool IgnoreStreamTableIndex_ = false;

    const TMkqlIOSpecs* Specs_ = nullptr;
    const NKikimr::NMiniKQL::THolderFactory* HolderFactoryPtr = nullptr;
    NKikimr::NMiniKQL::IStatsRegistry* JobStats_ = nullptr;
};

//////////////////////////////////////////////////////////////////////////////////////////////////////////

class TMkqlWriterImpl: public IMkqlWriterImpl {
protected:
    struct TOutput {
        TOutput(IOutputStream& streams, size_t blockCount, size_t blockSize, NKikimr::NMiniKQL::TStatTimer& timerWrite);
        TOutput(NYT::TRawTableWriterPtr stream, size_t blockSize, NKikimr::NMiniKQL::TStatTimer& timerWrite);

        THolder<NCommon::IBlockWriter> Writer_;
        NCommon::TOutputBuf Buf_;
    };

public:
    struct TEncoder;

    // Output streams must be valid while TMkqlWriterImpl exists
    TMkqlWriterImpl(const TVector<IOutputStream*>& streams, size_t blockCount, size_t blockSize);
    TMkqlWriterImpl(IOutputStream& stream, size_t blockCount, size_t blockSize);
    // client writer
    TMkqlWriterImpl(const TVector<NYT::TRawTableWriterPtr>& streams, size_t blockSize);
    TMkqlWriterImpl(NYT::TRawTableWriterPtr stream, size_t blockSize);

    ~TMkqlWriterImpl();

    void SetSpecs(const TMkqlIOSpecs& specs, const TVector<TString>& columns = {});

    void AddRow(const NKikimr::NUdf::TUnboxedValuePod row) override;
    void AddFlatRow(const NUdf::TUnboxedValuePod* row) override;

    void SetWriteLimit(ui64 limit) {
        WriteLimit = limit;
    }

    void Finish() override;
    void Abort() override;

    void Report();

protected:
    void WriteSkiffValue(NCommon::TOutputBuf& buf, NKikimr::NMiniKQL::TType* type, const NKikimr::NUdf::TUnboxedValuePod& value);
    virtual void DoFinish(bool abort);

protected:
    NKikimr::NMiniKQL::TStatTimer TimerEncode_;
    NKikimr::NMiniKQL::TStatTimer TimerWrite_;
    const TMkqlIOSpecs* Specs_ = nullptr;
    NKikimr::NMiniKQL::IStatsRegistry* JobStats_ = nullptr;
    TVector<THolder<TOutput>> Outputs_;
    bool IsFinished_ = false;
    TVector<THolder<TEncoder>> Encoders_;
#ifndef MKQL_DISABLE_CODEGEN
    NCodegen::ICodegen::TPtr Codegen_;
#endif
    TMaybe<ui64> WriteLimit;
};

//////////////////////////////////////////////////////////////////////////////////////////////////////////

class TMkqlInput: public THoldingStream<NYT::NDetail::TInputStreamProxy, IInputStream> {
    using TBase = THoldingStream<NYT::NDetail::TInputStreamProxy, IInputStream>;
public:
    TMkqlInput(::THolder<IInputStream> h)
        : TBase(std::move(h))
    {
    }
};

THolder<IInputStream> MakeStringInput(const TString& str, bool decompress);
THolder<IInputStream> MakeFileInput(const TString& file, bool decompress);

//////////////////////////////////////////////////////////////////////////////////////////////////////////

NKikimr::NUdf::TUnboxedValue DecodeYamr(TMkqlIOCache& specsCache, size_t tableIndex, const NYT::TYaMRRow& row);
void DecodeToYson(TMkqlIOCache& specsCache, size_t tableIndex, const NYT::TNode& value, IOutputStream& ysonOut);
void DecodeToYson(TMkqlIOCache& specsCache, size_t tableIndex, const NYT::TYaMRRow& value, IOutputStream& ysonOut);
void DecodeToYson(TMkqlIOCache& specsCache, size_t tableIndex, const NKikimr::NUdf::TUnboxedValuePod& value, IOutputStream& ysonOut);

} // NYql
