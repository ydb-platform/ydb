#pragma once
#include <ydb/library/yql/parser/pg_query_wrapper/contrib/protobuf/pg_query.pb-c.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

extern "C" {
struct ProtobufCMessage;
};

namespace NYql {

class IPGParseEvents {
public:
    virtual ~IPGParseEvents() = default;
    virtual void OnResult(const PgQuery__ParseResult* result) = 0;
    virtual void OnError(const TIssue& issue) = 0;
};

void PGParse(const TString& input, IPGParseEvents& events);

void PrintCProto(const ProtobufCMessage *message, IOutputStream& out);

}

