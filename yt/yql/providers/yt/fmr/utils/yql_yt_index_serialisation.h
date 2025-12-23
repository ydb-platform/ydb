#pragma once

#include <util/stream/str.h>
#include <util/stream/mem.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_parser_fragment_list_index.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_sort_helper.h>

namespace NYql::NFmr {

struct TSortedRowMetadata {
    TVector<TRowIndexMarkup> Rows;
    TVector<TString> KeyColumns;

    void Save(IOutputStream* buffer) const;
    void Load(IInputStream* buffer, TVector<TString> KeyColumns);

    bool operator == (const TSortedRowMetadata&) const = default;
};

void WriteVarUint64(IOutputStream* out, ui64 value);

void WriteVarUint32(IOutputStream* out, ui32 value);

void WriteString(IOutputStream* out, const TString& str);

ui64 ReadVarUint64(IInputStream* in);

ui32 ReadVarUint32(IInputStream* in);

TString ReadString(IInputStream* in);

}
