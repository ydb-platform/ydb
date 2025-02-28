#pragma once

#include <ydb-cpp-sdk/client/value/value.h>

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb/public/api/protos/ydb_table.pb.h>

#include <yql/essentials/minikql/mkql_alloc.h>

#include <util/generic/hash.h>
#include <util/stream/str.h>
#include <util/string/builder.h>

namespace NKikimr {
namespace NSysView {

class TCreateTableFormatter {
public:
    class TFormatFail : public yexception {
    public:
        Ydb::StatusIds::StatusCode Status;
        TString Error;

        TFormatFail(Ydb::StatusIds::StatusCode status,
            TString error = {})
            : Status(status)
            , Error(std::move(error))
        {}
    };

    class TResult {
    public:
        TResult(TString out)
            : Out(std::move(out))
            , Status(Ydb::StatusIds::SUCCESS)
        {}

        TResult(Ydb::StatusIds::StatusCode status, TString error)
            : Status(status)
            , Error(std::move(error))
        {}

        bool IsSuccess() const {
            return Status == Ydb::StatusIds::SUCCESS;
        }

        Ydb::StatusIds::StatusCode GetStatus() const {
            return Status;
        }

        const TString& GetError() const {
            return Error;
        }

        TString ExtractOut() {
            return std::move(Out);
        }
    private:
        TString Out;

        Ydb::StatusIds::StatusCode Status;
        TString Error;
    };

    TCreateTableFormatter()
    : Alloc(__LOCATION__)
    {
        Alloc.Release();
    }

    ~TCreateTableFormatter()
    {
        Alloc.Acquire();
    }

    TResult Format(const TString& tablePath, const NKikimrSchemeOp::TTableDescription& tableDesc, bool temporary);

private:

    void Format(const NKikimrSchemeOp::TColumnDescription& columnDesc);
    void Format(const NKikimrSchemeOp::TFamilyDescription& familyDesc);

    void Format(const Ydb::Table::TableIndex& index);
    void Format(const Ydb::Table::PartitioningSettings& partitionSettings);
    void Format(const Ydb::Table::ExplicitPartitions& explicitPartitions);
    void Format(const Ydb::Table::ReadReplicasSettings& readReplicasSettings);
    void Format(const Ydb::Table::TtlSettings& ttlSettings);

    void Format(ui64 expireAfterSeconds, std::optional<TString> storage = std::nullopt);

    void Format(const Ydb::TypedValue& value, bool isPartition = false);
    void FormatValue(NYdb::TValueParser& parser, bool isPartition = false, TString del = "");
    void FormatPrimitive(NYdb::TValueParser& parser);

    void EscapeName(const TString& str);
    void EscapeString(const TString& str);
    void EscapeBinary(const TString& str);


private:
    TStringStream Stream;
    NMiniKQL::TScopedAlloc Alloc;
};

} // NSysView
} // NKikimr
