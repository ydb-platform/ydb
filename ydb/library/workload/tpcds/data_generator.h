#pragma once

#include "tpcds.h"
#include "driver.h"
#include <library/cpp/object_factory/object_factory.h>
#include <util/string/printf.h>

extern "C" {
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/nulls.h>
    struct DECIMAL_T;
}

namespace NYdbWorkload {

class TTpcdsWorkloadDataInitializerGenerator: public TWorkloadDataInitializerBase {
public:
    TTpcdsWorkloadDataInitializerGenerator(const TTpcdsWorkloadParams& params);
    void ConfigureOpts(NLastGetopt::TOpts& opts) override;
    YDB_READONLY(double, Scale, 1);
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

        virtual void GenerateRows(TContexts& ctxs, TGuard<TAdaptiveLock>&& g) = 0;

        int TableNum;
        ui64 Generated = 0;
        TAdaptiveLock Lock;
    protected:
        static TString ConvertString(const char str[]);
        static TString ConvertDate(i64 val);
        static TString ConvertDecimal(const DECIMAL_T& val);

    private:
        TString GetFullTableName(const char* table) const;
        ds_key_t FirstRow = 1;
        ui64 StartPosition = 0;
        TDataPortionPtr FirstPortion;
        const TTpcdsWorkloadDataInitializerGenerator& Owner;
    };
};

template<class T>
class TTpcdsCsvItemWriter : public TCsvItemWriter<T> {
public:
    using TBase = TCsvItemWriter<T>;

    TTpcdsCsvItemWriter(IOutputStream& out, size_t size)
        : TBase(out)
    {
        NullCols.reserve(size);
    };

    void RegisterRow() {
        NullCols.emplace_back();
        for (auto key: ColKeys) {
            if (nullCheck(key)) {
                NullCols.back().emplace(key);
            }
        }
    }

    void RegisterField(TStringBuf name, int columnKey, TBase::TWriteFunction writeFunc) {
        TBase::RegisterField(name, writeFunc);
        ColKeys.emplace_back(columnKey);
    }

protected:
    void WriteImpl(const TBase::TItem& item, size_t itemIndex, const size_t fieldIndex) override {
        if (!NullCols[itemIndex].contains(ColKeys[fieldIndex])) {
            TBase::WriteImpl(item, itemIndex, fieldIndex);
        }
    }

private:
    using TNullCols = TSet<int>;
    TVector<TNullCols> NullCols;
    TVector<int> ColKeys;
};


#define CSV_WRITER_REGISTER_FIELD(writer, column_name, record_field, column_key) \
    writer.RegisterField(column_name, column_key, [](const decltype(writer)::TItem& item, IOutputStream& out) { \
        out << item.record_field; \
    });

#define CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, column_name, column_key) \
    CSV_WRITER_REGISTER_FIELD(writer, #column_name, column_name, column_key);

#define CSV_WRITER_REGISTER_FIELD_KEY(writer, column_name, record_field, column_key) \
    writer.RegisterField(column_name, column_key, [](const decltype(writer)::TItem& item, IOutputStream& out) { \
        if (item.record_field != -1) { \
            out << item.record_field; \
        }; \
    });

#define CSV_WRITER_REGISTER_SIMPLE_FIELD_KEY(writer, column_name, column_key) \
    CSV_WRITER_REGISTER_FIELD_KEY(writer, #column_name, column_name, column_key);

#define CSV_WRITER_REGISTER_FIELD_BOOL(writer, column_name, record_field, column_key) \
    writer.RegisterField(column_name, column_key, [](const decltype(writer)::TItem& item, IOutputStream& out) { \
        out << (item.record_field ? "Y" : "N"); \
    });

#define CSV_WRITER_REGISTER_SIMPLE_FIELD_BOOL(writer, column_name, column_key) \
    CSV_WRITER_REGISTER_FIELD_BOOL(writer, #column_name, column_name, column_key);

#define CSV_WRITER_REGISTER_FIELD_DATE(writer, column_name, record_field, column_key) \
    writer.RegisterField(column_name, column_key, [](const decltype(writer)::TItem& item, IOutputStream& out) { \
        out << TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::ConvertDate(item.record_field); \
    });

#define CSV_WRITER_REGISTER_SIMPLE_FIELD_DATE(writer, column_name, column_key) \
    CSV_WRITER_REGISTER_FIELD_DATE(writer, #column_name, column_name, column_key);

#define CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, column_name, record_field, column_key) \
    writer.RegisterField(column_name, column_key, [](const decltype(writer)::TItem& item, IOutputStream& out) { \
        out << TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::ConvertDecimal(item.record_field); \
    });

#define CSV_WRITER_REGISTER_SIMPLE_FIELD_DECIMAL(writer, column_name, column_key) \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name, column_name, column_key);

#define CSV_WRITER_REGISTER_FIELD_STRING(writer, column_name, record_field, column_key) \
    writer.RegisterField(column_name, column_key, [](const decltype(writer)::TItem& item, IOutputStream& out) { \
        out << TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::ConvertString(item.record_field); \
    });

#define CSV_WRITER_REGISTER_SIMPLE_FIELD_STRING(writer, column_name, column_key) \
    CSV_WRITER_REGISTER_FIELD_STRING(writer, #column_name, column_name, column_key);

#define CSV_WRITER_REGISTER_ADDRESS_FIELDS(writer, column_name_prefix, record_addr_field, column_key_prefix) \
    CSV_WRITER_REGISTER_FIELD(writer, #column_name_prefix "_street_number", record_addr_field.street_num, column_key_prefix ## _STREET_NUM); \
    writer.RegisterField(#column_name_prefix "_street_name", column_key_prefix ## _STREET_NAME1, [](const decltype(writer)::TItem& item, IOutputStream& out) { \
        auto stName = item.record_addr_field.street_name2 ? \
            Sprintf("%s %s", item.record_addr_field.street_name1, item.record_addr_field.street_name2) : \
            TString(item.record_addr_field.street_name1); \
        out << TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::ConvertString(stName.c_str()); \
    }); \
    CSV_WRITER_REGISTER_FIELD_STRING(writer, #column_name_prefix "_street_type", record_addr_field.street_type, column_key_prefix ## _STREET_TYPE); \
    CSV_WRITER_REGISTER_FIELD_STRING(writer, #column_name_prefix "_suite_number", record_addr_field.suite_num, column_key_prefix ## _SUITE_NUM); \
    CSV_WRITER_REGISTER_FIELD_STRING(writer, #column_name_prefix "_city", record_addr_field.city, column_key_prefix ## _CITY); \
    CSV_WRITER_REGISTER_FIELD_STRING(writer, #column_name_prefix "_county", record_addr_field.county, column_key_prefix ## _COUNTY); \
    CSV_WRITER_REGISTER_FIELD_STRING(writer, #column_name_prefix "_state", record_addr_field.state, column_key_prefix ## _STATE); \
    writer.RegisterField(#column_name_prefix "_zip", column_key_prefix ## _ZIP, [](const decltype(writer)::TItem& item, IOutputStream& out) { \
        out << Sprintf("%05d", item.record_addr_field.zip); \
    }); \
    CSV_WRITER_REGISTER_FIELD_STRING(writer, #column_name_prefix "_country", record_addr_field.country, column_key_prefix ## _COUNTRY); \
    CSV_WRITER_REGISTER_FIELD(writer, #column_name_prefix "_gmt_offset", record_addr_field.gmt_offset, column_key_prefix ## _GMT_OFFSET);

#define CSV_WRITER_REGISTER_PRICING_FIELDS(writer, column_name_prefix, record_pricing_field, ext_fields, column_key_prefix) \
    CSV_WRITER_REGISTER_FIELD(writer, #column_name_prefix "_quantity", record_pricing_field.quantity, column_key_prefix ## _PRICING_QUANTITY); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_wholesale_cost", record_pricing_field.wholesale_cost, column_key_prefix ## _PRICING_WHOLESALE_COST); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_list_price", record_pricing_field.list_price, column_key_prefix ## _PRICING_LIST_PRICE); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_sales_price", record_pricing_field.sales_price, column_key_prefix ## _PRICING_SALES_PRICE); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_ext_discount_amt", record_pricing_field.ext_discount_amt, column_key_prefix ## _PRICING_EXT_DISCOUNT_AMOUNT); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_ext_sales_price", record_pricing_field.ext_sales_price, column_key_prefix ## _PRICING_EXT_SALES_PRICE); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_ext_wholesale_cost", record_pricing_field.ext_wholesale_cost, column_key_prefix ## _PRICING_EXT_WHOLESALE_COST); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_ext_list_price", record_pricing_field.ext_list_price, column_key_prefix ## _PRICING_EXT_LIST_PRICE); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_ext_tax", record_pricing_field.ext_tax, column_key_prefix ## _PRICING_EXT_TAX); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_coupon_amt", record_pricing_field.coupon_amt, column_key_prefix ## _PRICING_COUPON_AMT); \
    if (ext_fields) { \
        CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_ext_ship_cost", record_pricing_field.ext_ship_cost, column_key_prefix ## _PRICING_EXT_SHIP_COST); \
    } \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_net_paid", record_pricing_field.net_paid, column_key_prefix ## _PRICING_NET_PAID); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_net_paid_inc_tax", record_pricing_field.net_paid_inc_tax, column_key_prefix ## _PRICING_NET_PAID_INC_TAX); \
    if (ext_fields) { \
        CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_net_paid_inc_ship", record_pricing_field.net_paid_inc_ship, column_key_prefix ## _PRICING_NET_PAID_INC_SHIP); \
        CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_net_paid_inc_ship_tax", record_pricing_field.net_paid_inc_ship_tax, column_key_prefix ## _PRICING_NET_PAID_INC_SHIP_TAX); \
    } \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_net_profit", record_pricing_field.net_profit, column_key_prefix ## _PRICING_NET_PROFIT);

#define CSV_WRITER_REGISTER_RETURN_PRICING_FIELDS(writer, column_name_prefix, record_pricing_field, column_key_prefix) \
    CSV_WRITER_REGISTER_FIELD(writer, #column_name_prefix "_return_quantity", record_pricing_field.quantity, column_key_prefix ## _PRICING_QUANTITY); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_return_amt", record_pricing_field.net_paid, column_key_prefix ## _PRICING_NET_PAID); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_return_tax", record_pricing_field.ext_tax, column_key_prefix ## _PRICING_EXT_TAX); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_return_amt_inc_tax", record_pricing_field.net_paid_inc_tax, column_key_prefix ## _PRICING_NET_PAID_INC_TAX); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_fee", record_pricing_field.fee, column_key_prefix ## _PRICING_FEE); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_return_ship_cost", record_pricing_field.ext_ship_cost, column_key_prefix ## _PRICING_EXT_SHIP_COST); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_refunded_cash", record_pricing_field.refunded_cash, column_key_prefix ## _PRICING_REFUNDED_CASH); \
    CSV_WRITER_REGISTER_FIELD_DECIMAL(writer, #column_name_prefix "_reversed_charge", record_pricing_field.reversed_charge, column_key_prefix ## _PRICING_REVERSED_CHARGE); \

}
