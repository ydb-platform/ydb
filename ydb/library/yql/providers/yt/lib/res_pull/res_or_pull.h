#pragma once

#include <ydb/library/yql/providers/yt/codec/yt_codec_io.h>
#include <ydb/library/yql/public/udf/udf_value.h>

#include <ydb/library/yql/minikql/mkql_node.h>

#include <library/cpp/yson/writer.h>

#include <util/stream/length.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/ptr.h>
#include <util/generic/noncopyable.h>

namespace NYql {

class TExecuteResOrPull : public TNonCopyable {
public:
    TExecuteResOrPull(TMaybe<ui64> rowLimit, TMaybe<ui64> byteLimit, const TMaybe<TVector<TString>>& columns);

    bool HasCapacity() const {
        return (!Rows || Row < *Rows) && (!Bytes || Out->Counter() < *Bytes);
    }

    bool IsTruncated() const {
        return Truncated;
    }

    ui64 GetWrittenSize() const;

    ui64 GetWrittenRows() const {
        return Row;
    }

    TString Finish();
    TMaybe<ui64> GetRowsLimit() const {
        return Rows;
    }

    void SetListResult() {
        if (!IsList) {
            IsList = true;
            Writer->OnBeginList();
        }
    }

    const TMaybe<TVector<TString>>& GetColumns() const {
        return Columns;
    }

    bool WriteNext(TStringBuf val);

    template <class TRec>
    bool WriteNext(TMkqlIOCache& specCache, const TRec& rec, ui32 tableIndex) {
        if (!HasCapacity()) {
            Truncated = true;
            return false;
        }
        NYql::DecodeToYson(specCache, tableIndex, rec, *Out);
        ++Row;
        return true;
    }

    void WriteValue(const NKikimr::NUdf::TUnboxedValue& value, NKikimr::NMiniKQL::TType* type);

protected:
    const TMaybe<ui64> Rows;
    const TMaybe<ui64> Bytes;
    const TMaybe<TVector<TString>> Columns;
    TString Result;
    THolder<TCountingOutput> Out;
    THolder<NYson::TYsonWriter> Writer;
    bool IsList;
    bool Truncated;
    ui64 Row;
};

}
