#include "client.h"

#include <ydb/library/yql/utils/yql_panic.h>

namespace NYql::NConnector {
    TDescribeTableResult::TPtr TClientMock::DescribeTable(const NApi::TDescribeTableRequest&) {
        auto out = std::make_shared<TDescribeTableResult>();
        out->Error.set_status(Ydb::StatusIds_StatusCode::StatusIds_StatusCode_SUCCESS);
        auto schema = &out->Schema;

        {
            auto column = schema->add_columns();
            column->set_name("Day");
            column->mutable_type()->set_type_id(Ydb::Type::INT8);
        }
        {
            auto column = schema->add_columns();
            column->set_name("Month");
            column->mutable_type()->set_type_id(Ydb::Type::INT8);
        }
        {
            auto column = schema->add_columns();
            column->set_name("Year");
            column->mutable_type()->set_type_id(Ydb::Type::INT16);
        }

        return out;
    }

    arrow::Status TClientMock::PrepareRecordBatch(std::shared_ptr<arrow::RecordBatch>& recordBatch) {
        arrow::Int8Builder int8builder;
        int8_t days_raw[5] = {1, 12, 17, 23, 28};
        ARROW_RETURN_NOT_OK(int8builder.AppendValues(days_raw, 5));

        std::shared_ptr<arrow::Array> days;
        ARROW_ASSIGN_OR_RAISE(days, int8builder.Finish());

        int8_t months_raw[5] = {1, 3, 5, 7, 1};
        ARROW_RETURN_NOT_OK(int8builder.AppendValues(months_raw, 5));

        std::shared_ptr<arrow::Array> months;
        ARROW_ASSIGN_OR_RAISE(months, int8builder.Finish());

        arrow::Int16Builder int16builder;
        int16_t years_raw[5] = {1990, 2000, 1995, 2000, 1995};
        ARROW_RETURN_NOT_OK(int16builder.AppendValues(years_raw, 5));
        std::shared_ptr<arrow::Array> years;
        ARROW_ASSIGN_OR_RAISE(years, int16builder.Finish());

        std::shared_ptr<arrow::Field> field_day, field_month, field_year;
        std::shared_ptr<arrow::Schema> schema;

        field_day = arrow::field("Day", arrow::int8());
        field_month = arrow::field("Month", arrow::int8());
        field_year = arrow::field("Year", arrow::int16());

        schema = arrow::schema({field_day, field_month, field_year});

        recordBatch = arrow::RecordBatch::Make(schema, days->length(), {days, months, years});

        return arrow::Status::OK();
    }

    TListSplitsResult::TPtr TClientMock::ListSplits(const NApi::TListSplitsRequest& request) {
        YQL_ENSURE(request.selects().size() == 1);

        auto out = std::make_shared<TListSplitsResult>();
        out->Error.set_status(::Ydb::StatusIds_StatusCode::StatusIds_StatusCode_SUCCESS);

        NApi::TSplit split;
        split.mutable_select()->CopyFrom(request.selects()[0]);

        out->Splits.push_back(split);
        return out;
    }

    TReadSplitsResult::TPtr TClientMock::ReadSplits(const NApi::TReadSplitsRequest&) {
        auto out = std::make_shared<TReadSplitsResult>();
        out->Error.set_status(::Ydb::StatusIds_StatusCode::StatusIds_StatusCode_SUCCESS);

        out->RecordBatches.resize(1);
        arrow::Status st = PrepareRecordBatch(out->RecordBatches[0]);
        if (!st.ok()) {
            ythrow yexception() << st.ok();
        }

        return out;
    }

    IClient::TPtr MakeTClientMock() {
        return std::make_shared<TClientMock>();
    }

}
