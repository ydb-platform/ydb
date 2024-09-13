#pragma once

#include "tpcds.h"
#include <library/cpp/object_factory/object_factory.h>
#include <util/string/printf.h>

extern "C" struct DECIMAL_T;

namespace NYdbWorkload {

class TTpcdsWorkloadDataInitializerGenerator: public TWorkloadDataInitializerBase {
public:
    TTpcdsWorkloadDataInitializerGenerator(const TTpcdsWorkloadParams& params);
    void ConfigureOpts(NLastGetopt::TOpts& opts) override;
    YDB_READONLY(ui64, Scale, 1);
    YDB_READONLY_DEF(TSet<TString>, Tables);
    YDB_READONLY(ui32, ProcessIndex, 0);
    YDB_READONLY(ui32, ProcessCount, 1);
protected:
    TBulkDataGeneratorList DoGetBulkInitialData() override;

public:
    class TBulkDataGenerator : public IBulkDataGenerator {
    public:
        using TFactory = NObjectFactory::TParametrizedObjectFactory<TBulkDataGenerator, TString, const TTpcdsWorkloadDataInitializerGenerator&>;

        TBulkDataGenerator(const TTpcdsWorkloadDataInitializerGenerator& owner, int tableNum);
        TDataPortions GenerateDataPortion() override;

        bool operator < (const TBulkDataGenerator& other) const {
            return TableNum < other.TableNum;
        }

    protected:
        class TContext {
        public:
            TContext(const TBulkDataGenerator& owner, int tableNum);
            NYdb::TValueBuilder& GetBuilder();
            TStringBuilder& GetCsv();
            void AppendPortions(TDataPortions& result);
            YDB_ACCESSOR(ui64, Count, 0);
            YDB_ACCESSOR(ui64, Start, 0);

        private:
            THolder<NYdb::TValueBuilder> Builder;
            TStringBuilder Csv;
            const TBulkDataGenerator& Owner;
            int TableNum;
        };

        using TContexts = TVector<TContext>;

        virtual void GenerateRows(TContexts& ctxs) = 0;

        int TableNum;
        ui64 Generated = 0;
        TAdaptiveLock Lock;
    protected:
        static TString ConvertString(const char str[]);
        static TString ConvertDate(i64 val);
        static TString ConvertDecimal(const DECIMAL_T& val);

    private:
        TString GetFullTableName(const char* table) const;
        static ui64 CalcCountToGenerate(const TTpcdsWorkloadDataInitializerGenerator& owner, int tableNum, bool useState);
        const TTpcdsWorkloadDataInitializerGenerator& Owner;
        ui64 TableSize;
    };
};

template<class T>
class TCsvItemWriter {
public:
    using TItem = T;
    using TWriteFunction = std::function<void(const TItem&, IOutputStream&)>;
    explicit TCsvItemWriter(IOutputStream& out)
        : Out(out)
    {}

    void RegisterField(TStringBuf name, TWriteFunction writeFunc) {
        Fields.emplace_back(name, writeFunc);
    }
    void Write(const TVector<TItem>& items) {
        for(const auto& field: Fields) {
            Out << field.Name;
            if (&field + 1 != Fields.end()) {
                Out << '|';
            }
        }
        Out << Endl;
        for(const auto& item: items) {
            for(const auto& field: Fields) {
                field.WriteFunction(item, Out);
                if (&field + 1 != Fields.end()) {
                    Out << '|';
                }
            }
            Out << Endl;
        }
    }

private:
    struct TField {
        TField(TStringBuf name, TWriteFunction func)
            : Name(name)
            , WriteFunction(func)
        {}
        TStringBuf Name;
        TWriteFunction WriteFunction;
    };
    TVector<TField> Fields;
    IOutputStream& Out;
};

#define CSV_WRITER_REGISTER_FIELD(writer, column_name, record_field) \
    writer.RegisterField(column_name, [](const decltype(writer)::TItem& item, IOutputStream& out) { \
        out << item.record_field; \
    });

#define CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, column_name) \
    CSV_WRITER_REGISTER_FIELD(writer, #column_name, column_name);

#define CSV_WRITER_REGISTER_FIELD_KEY(writer, column_name, record_field) \
    writer.RegisterField(column_name, [](const decltype(writer)::TItem& item, IOutputStream& out) { \
        if (item.record_field != -1) { \
            out << item.record_field; \
        }; \
    });

#define CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, column_name) \
    CSV_WRITER_REGISTER_FIELD_KEY(writer, #column_name, column_name);

#define CSV_WRITER_REGISTER_FIELD_BOOL(writer, column_name, record_field) \
    writer.RegisterField(column_name, [](const decltype(writer)::TItem& item, IOutputStream& out) { \
        out << (item.record_field ? "Y" : "N"); \
    });

#define CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, column_name) \
    CSV_WRITER_REGISTER_FIELD_BOOL(writer, #column_name, column_name);

#define CSV_WRITER_REGISTER_FIELD_DATE(writer, column_name, record_field) \
    writer.RegisterField(column_name, [](const decltype(writer)::TItem& item, IOutputStream& out) { \
        out << TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::ConvertDate(item.record_field); \
    });

#define CSV_WRITER_REGISTER_SIMPLE_FIELD_DATE(writer, column_name) \
    CSV_WRITER_REGISTER_FIELD_DATE(writer, #column_name, column_name);

#define CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, column_name, record_field) \
    writer.RegisterField(column_name, [](const decltype(writer)::TItem& item, IOutputStream& out) { \
        out << TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::ConvertDecimal(item.record_field); \
    });

#define CSV_WRITER_REGISTER_SIMPLE_FIELD_DECIMAL(writer, column_name) \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name, column_name);

#define CSV_WRITER_REGISTER_FIELD_STRING(writer, column_name, record_field) \
    writer.RegisterField(column_name, [](const decltype(writer)::TItem& item, IOutputStream& out) { \
        out << TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::ConvertString(item.record_field); \
    });

#define CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, column_name) \
    CSV_WRITER_REGISTER_FIELD_STRING(writer, #column_name, column_name);

#define CSV_WRITER_REGISTER_ADDRESS_FIELDS(writer, column_name_prefix, record_addr_field) \
    CSV_WRITER_REGISTER_FIELD(writer, #column_name_prefix "_street_number", record_addr_field.street_num); \
    writer.RegisterField(#column_name_prefix "_street_name", [](const decltype(writer)::TItem& item, IOutputStream& out) { \
        auto stName = item.record_addr_field.street_name2 ? \
            Sprintf("%s %s", item.record_addr_field.street_name1, item.record_addr_field.street_name2) : \
            TString(item.record_addr_field.street_name1); \
        out << TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::ConvertString(stName.c_str()); \
    }); \
    CSV_WRITER_REGISTER_FIELD_STRING(writer, #column_name_prefix "_street_type", record_addr_field.street_type); \
    CSV_WRITER_REGISTER_FIELD_STRING(writer, #column_name_prefix "_suite_number", record_addr_field.suite_num); \
    CSV_WRITER_REGISTER_FIELD_STRING(writer, #column_name_prefix "_city", record_addr_field.city); \
    CSV_WRITER_REGISTER_FIELD_STRING(writer, #column_name_prefix "_county", record_addr_field.county); \
    CSV_WRITER_REGISTER_FIELD_STRING(writer, #column_name_prefix "_state", record_addr_field.state); \
    writer.RegisterField(#column_name_prefix "_zip", [](const decltype(writer)::TItem& item, IOutputStream& out) { \
        out << Sprintf("%05d", item.record_addr_field.zip); \
    }); \
    CSV_WRITER_REGISTER_FIELD_STRING(writer, #column_name_prefix "_country", record_addr_field.country); \
    CSV_WRITER_REGISTER_FIELD(writer, #column_name_prefix "_gmt_offset", record_addr_field.gmt_offset);

#define CSV_WRITER_REGISTER_PRICING_FIELDS(writer, column_name_prefix, record_pricing_field) \
    CSV_WRITER_REGISTER_FIELD(writer, #column_name_prefix "_quantity", record_pricing_field.quantity); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_wholesale_cost", record_pricing_field.wholesale_cost); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_list_price", record_pricing_field.list_price); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_sales_price", record_pricing_field.sales_price); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_ext_discount_amt", record_pricing_field.ext_discount_amt); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_ext_sales_price", record_pricing_field.ext_sales_price); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_ext_wholesale_cost", record_pricing_field.ext_wholesale_cost); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_ext_list_price", record_pricing_field.ext_list_price); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_ext_tax", record_pricing_field.ext_tax); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_coupon_amt", record_pricing_field.coupon_amt); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_ext_ship_cost", record_pricing_field.ext_ship_cost); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_net_paid", record_pricing_field.net_paid); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_net_paid_inc_tax", record_pricing_field.net_paid_inc_tax); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_net_paid_inc_ship", record_pricing_field.net_paid_inc_ship); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_net_paid_inc_ship_tax", record_pricing_field.net_paid_inc_ship_tax); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_net_profit", record_pricing_field.net_profit);

#define CSV_WRITER_REGISTER_RETURN_PRICING_FIELDS(writer, column_name_prefix, record_pricing_field) \
    CSV_WRITER_REGISTER_FIELD(writer, #column_name_prefix "_return_quantity", record_pricing_field.quantity); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_return_amt", record_pricing_field.net_paid); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_return_tax", record_pricing_field.ext_tax); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_return_amt_inc_tax", record_pricing_field.net_paid_inc_tax); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_return_ship_cost", record_pricing_field.ship_cost); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_fee", record_pricing_field.fee); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_refunded_cash", record_pricing_field.refunded_cash); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_reversed_charge", record_pricing_field.reversed_charge); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_net_loss", record_pricing_field.net_loss);

}
