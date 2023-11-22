#pragma once

#include <ydb/library/yql/providers/yt/codec/yt_codec_io.h>
#include <ydb/library/yql/public/udf/udf_value.h>

#include <ydb/library/yql/minikql/mkql_node.h>

#include <library/cpp/yson/node/node_io.h>

#include <library/cpp/yson/writer.h>

#include <util/stream/length.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/ptr.h>
#include <util/generic/noncopyable.h>

namespace NYql {

///////////////////////////////////////////////////////////////////////////////
// IExecuteResOrPull
///////////////////////////////////////////////////////////////////////////////
class IExecuteResOrPull : public TNonCopyable {
public:
    IExecuteResOrPull(TMaybe<ui64> rowLimit, TMaybe<ui64> byteLimit, const TMaybe<TVector<TString>>& columns)
        : Rows(rowLimit)
        , Bytes(byteLimit)
        , Columns(columns)
        , Out(new THoldingStream<TCountingOutput>(THolder(new TStringOutput(Result))))
        , IsList(false)
        , Truncated(false)
        , Row(0)
    {
    }
    virtual ~IExecuteResOrPull() = default;

    bool HasCapacity() const {
        return (!Rows || Row < *Rows) && (!Bytes || Out->Counter() < *Bytes);
    }

    bool IsTruncated() const {
        return Truncated;
    }

    ui64 GetWrittenSize() const {
        YQL_ENSURE(Out, "GetWritten() must be callled before Finish()");
        return Out->Counter();
    }

    ui64 GetWrittenRows() const {
        return Row;
    }

    TMaybe<ui64> GetRowsLimit() const {
        return Rows;
    }

    const TMaybe<TVector<TString>>& GetColumns() const {
        return Columns;
    }

    std::pair<TString, bool> Make() {
        return {Finish(), IsTruncated()};
    }

    virtual TString Finish() = 0;

    virtual void SetListResult() = 0;

    virtual bool WriteNext(const NYT::TNode& item) = 0;

    virtual bool WriteNext(TMkqlIOCache& specsCache, const NUdf::TUnboxedValue& rec, ui32 tableIndex) = 0;
    virtual bool WriteNext(TMkqlIOCache& specsCache, const NYT::TYaMRRow& rec, ui32 tableIndex) = 0;
    virtual bool WriteNext(TMkqlIOCache& specsCache, const NYT::TNode& rec, ui32 tableIndex) = 0;

    virtual void WriteValue(const NKikimr::NUdf::TUnboxedValue& value, NKikimr::NMiniKQL::TType* type) = 0;

protected:
    const TMaybe<ui64> Rows;
    const TMaybe<ui64> Bytes;
    const TMaybe<TVector<TString>> Columns;
    TString Result;
    THolder<TCountingOutput> Out;
    bool IsList;
    bool Truncated;
    ui64 Row;
};

///////////////////////////////////////////////////////////////////////////////
// TYsonExecuteResOrPull
///////////////////////////////////////////////////////////////////////////////
class TYsonExecuteResOrPull : public IExecuteResOrPull {
public:
    TYsonExecuteResOrPull(TMaybe<ui64> rowLimit, TMaybe<ui64> byteLimit, const TMaybe<TVector<TString>>& columns);
    ~TYsonExecuteResOrPull() = default;

    TString Finish() override;

    void SetListResult() override;

    bool WriteNext(const NYT::TNode& item) override;

    bool WriteNext(TMkqlIOCache& specsCache, const NUdf::TUnboxedValue& rec, ui32 tableIndex) override;
    bool WriteNext(TMkqlIOCache& specsCache, const NYT::TYaMRRow& rec, ui32 tableIndex) override;
    bool WriteNext(TMkqlIOCache& specsCache, const NYT::TNode& rec, ui32 tableIndex) override;

    void WriteValue(const NKikimr::NUdf::TUnboxedValue& value, NKikimr::NMiniKQL::TType* type) override;
protected:
    THolder<NYson::TYsonWriter> Writer;
};

///////////////////////////////////////////////////////////////////////////////
// TSkiffExecuteResOrPull
///////////////////////////////////////////////////////////////////////////////
class TSkiffExecuteResOrPull : public IExecuteResOrPull {
public:
    TSkiffExecuteResOrPull(TMaybe<ui64> rowLimit, TMaybe<ui64> byteLimit, NCommon::TCodecContext& codecCtx, const NKikimr::NMiniKQL::THolderFactory& holderFactory, const NYT::TNode& attrs, const TString& optLLVM, const TVector<TString>& columns = {});
    ~TSkiffExecuteResOrPull() = default;

    TString Finish() override;

    void SetListResult() override;

    bool WriteNext(const NYT::TNode& item) override;

    bool WriteNext(TMkqlIOCache& specsCache, const NUdf::TUnboxedValue& rec, ui32 tableIndex) override;
    bool WriteNext(TMkqlIOCache& specsCache, const NYT::TYaMRRow& rec, ui32 tableIndex) override;
    bool WriteNext(TMkqlIOCache& specsCache, const NYT::TNode& rec, ui32 tableIndex) override;

    void WriteValue(const NKikimr::NUdf::TUnboxedValue& value, NKikimr::NMiniKQL::TType* type) override;
protected:
    const NKikimr::NMiniKQL::THolderFactory& HolderFactory;

    TMkqlIOSpecs Specs;
    TMkqlWriterImpl SkiffWriter;
};

}
