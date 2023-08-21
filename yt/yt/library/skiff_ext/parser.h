#pragma once

#include <library/cpp/skiff/skiff_schema.h>

#include <yt/yt/core/concurrency/coroutine.h>
#include <yt/yt/core/misc/coro_pipe.h>

#include <util/generic/buffer.h>

namespace NYT::NSkiffExt {

////////////////////////////////////////////////////////////////////////////////

template <class TConsumer>
class TSkiffMultiTableParser
{
public:
    TSkiffMultiTableParser(
        TConsumer* consumer,
        NSkiff::TSkiffSchemaList schemaList,
        const std::vector<TSkiffTableColumnIds>& tablesColumnIds,
        const TString& rangeIndexColumnName,
        const TString& rowIndexColumnName);

    ~TSkiffMultiTableParser();

    void Read(TStringBuf data);
    void Finish();

    ui64 GetReadBytesCount();

private:
    class TImpl;
    std::unique_ptr<TImpl> ParserImpl_;

    TCoroPipe ParserCoroPipe_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSkiffExt

#define PARSER_INL_H_
#include "parser-inl.h"
#undef PARSER_INL_H_
