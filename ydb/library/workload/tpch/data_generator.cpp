#include "data_generator.h"
#include "driver.h"
#include <util/folder/path.h>
#include <util/string/printf.h>

namespace NYdbWorkload {

TTpchWorkloadDataInitializerGenerator::TTpchWorkloadDataInitializerGenerator(const TTpchWorkloadParams& params)
    : TWorkloadDataInitializerBase("generator", "Generate TPC-H dataset by native generator.", params)
{}

void TTpchWorkloadDataInitializerGenerator::ConfigureOpts(NLastGetopt::TOpts& opts) {
    TWorkloadDataInitializerBase::ConfigureOpts(opts);
    opts.AddLongOption("scale", "Sets the percentage of the benchmark's data size and workload to use, relative to full scale.")
        .DefaultValue(Scale).StoreResult(&Scale);
    opts.AddLongOption("tables", "Commaseparated list of tables for generate. Empty means all tables.\n"
            "Enabled tables: " + JoinSeq(", ", TBulkDataGenerator::TFactory::GetRegisteredKeys()))
        .Handler1T<TStringBuf>([this](TStringBuf arg) {
            StringSplitter(arg).SplitBySet(", ").SkipEmpty().AddTo(&Tables);
            const auto keys = TBulkDataGenerator::TFactory::GetRegisteredKeys();
            for (const auto& table: Tables) {
                if (!keys.contains(table)) {
                    throw yexception() << "Ivalid table for generate: " << table;
                }

            }
        });
    opts.AddLongOption('C', "proccess-count", "Count of parallel processes (for multiprocess runing).")
        .DefaultValue(ProcessCount).StoreResult(&ProcessCount);
    opts.AddLongOption('i', "proccess-index", "Zerobased index of parallel processes (for multiprocess runing).")
        .DefaultValue(ProcessIndex).StoreResult(&ProcessIndex);
}

TBulkDataGeneratorList TTpchWorkloadDataInitializerGenerator::DoGetBulkInitialData() {
    InitTpchGen(std::ceil(GetScale()));
    const auto tables = GetTables() ? GetTables() : TBulkDataGenerator::TFactory::GetRegisteredKeys();
    TVector<std::shared_ptr<TBulkDataGenerator>> gens;
    for (const auto& table: tables) {
        if (GetTables().empty() && table.StartsWith("__")) {
            continue;
        }
        gens.emplace_back(TBulkDataGenerator::TFactory::Construct(table, *this));
    }
    SortBy(gens, [](const auto& p) -> const auto& {return *p;});
    return TBulkDataGeneratorList(gens.begin(), gens.end());
}

TTpchWorkloadDataInitializerGenerator::TBulkDataGenerator::TContext::TContext(const TBulkDataGenerator& owner, int tableNum, TGeneratorStateProcessor* state)
    : Owner(owner)
    , TableNum(tableNum)
    , State(state)
{}

NYdb::TValueBuilder& TTpchWorkloadDataInitializerGenerator::TBulkDataGenerator::TContext::GetBuilder() {
    if (!Builder) {
        Builder = MakeHolder<NYdb::TValueBuilder>();
        Builder->BeginList();
    }
    return *Builder;
}

TStringBuilder& TTpchWorkloadDataInitializerGenerator::TBulkDataGenerator::TContext::GetCsv() {
    return Csv;
}

void TTpchWorkloadDataInitializerGenerator::TBulkDataGenerator::TContext::AppendPortions(TDataPortions& result) {
    const auto path = Owner.GetFullTableName(tdefs[TableNum].name);
    if (Builder) {
        Builder->EndList();
        result.push_back(MakeIntrusive<TDataPortionWithState>(
            State,
            path,
            tdefs[TableNum].name,
            Builder->Build(),
            Start - Owner.FirstRow,
            Count
        ));
    } else if (Csv) {
        result.push_back(MakeIntrusive<TDataPortionWithState>(
            State,
            path,
            tdefs[TableNum].name,
            TDataPortion::TCsv(std::move(Csv), TWorkloadGeneratorBase::PsvFormatString),
            Start - Owner.FirstRow,
            Count
        ));
    }
}

TString TTpchWorkloadDataInitializerGenerator::TBulkDataGenerator::GetFullTableName(const char* table) const {
    return Owner.Params.GetFullTableName(table);
}

TTpchWorkloadDataInitializerGenerator::TBulkDataGenerator::TBulkDataGenerator(const TTpchWorkloadDataInitializerGenerator& owner, int tableNum)
    : IBulkDataGenerator(tdefs[tableNum].name, 0)
    , TableNum(tableNum)
    , Owner(owner)
{
    if (TableNum == NONE) {
        return;
    }
    if (tableNum >= NATION) {
        Size = owner.GetProcessIndex() ? 0 : tdefs[tableNum].base;
    } else {
        DSS_HUGE extraRows = 0;
        Size = SetState(TableNum, Owner.GetScale(), Owner.GetProcessCount(), Owner.GetProcessIndex() + 1, &extraRows);
        FirstRow += Size * Owner.GetProcessIndex();
        if (Owner.GetProcessIndex() + 1 == Owner.GetProcessCount()) {
            Size += extraRows;
        }
    }
    if (!!Owner.StateProcessor) {
        if (const auto* state = MapFindPtr(Owner.StateProcessor->GetState(), GetName())) {
            Generated = state->Position;
            FirstPortion = MakeIntrusive<TDataPortion>(
                GetFullTableName(tdefs[TableNum].name),
                TDataPortion::TSkip(),
                Generated
            );
            GenSeed(TableNum, Generated);
        }
    }
}

TTpchWorkloadDataInitializerGenerator::TBulkDataGenerator::TDataPortions TTpchWorkloadDataInitializerGenerator::TBulkDataGenerator::GenerateDataPortion() {
    TDataPortions result;
    if (GetSize() == 0) {
        return result;
    }
    TContexts ctxs;
    ctxs.emplace_back(*this, TableNum, Owner.StateProcessor.Get());
    if (tdefs[TableNum].child != NONE) {
        ctxs.emplace_back(*this, tdefs[TableNum].child, nullptr);
    }

    auto g = Guard(Lock);
    if (FirstPortion) {
        result.emplace_back(std::move(FirstPortion));
    }
    const auto count = GetSize() > Generated ? std::min(ui64(GetSize() - Generated), Owner.Params.BulkSize) : 0;
    if (!count) {
        return result;
    }
    ctxs.front().SetCount(count);
    ctxs.front().SetStart(FirstRow + Generated);
    Generated += count;
    GenerateRows(ctxs, std::move(g));
    for(auto& ctx: ctxs) {
        ctx.AppendPortions(result);
    }
    return result;
}

#define CSV_WRITER_REGISTER_FIELD(writer, column_name, record_field) \
    writer.RegisterField(column_name, [](const decltype(writer)::TItem& item, IOutputStream& out) { \
        out << item.record_field; \
    });

#define CSV_WRITER_REGISTER_SIMPLE_FIELD(writer, column_name) \
    CSV_WRITER_REGISTER_FIELD(writer, #column_name, column_name);

#define CSV_WRITER_REGISTER_FIELD_DATE(writer, column_name, record_field) \
    writer.RegisterField(column_name, [](const decltype(writer)::TItem& item, IOutputStream& out) { \
        out << item.record_field; \
    });

#define CSV_WRITER_REGISTER_FIELD_COMMENT(writer, prefix) \
    writer.RegisterField(#prefix "_comment", [](const decltype(writer)::TItem& item, IOutputStream& out) { \
        out << TStringBuf(item.comment, item.clen); \
    });

#define CSV_WRITER_REGISTER_FIELD_MONEY(writer, column_name, record_field) \
    writer.RegisterField(column_name, [](const decltype(writer)::TItem& item, IOutputStream& out) { \
        int cents = item.record_field; \
        if (cents < 0) { \
            out << "-"; \
            cents = -cents; \
        } \
        int dollars = cents / 100; \
        cents %= 100; \
        out << Sprintf("%d.%02d", dollars, cents); \
    });

class TBulkDataGeneratorOrderLine : public TTpchWorkloadDataInitializerGenerator::TBulkDataGenerator {
public:
    explicit TBulkDataGeneratorOrderLine(const TTpchWorkloadDataInitializerGenerator& owner)
        : TBulkDataGenerator(owner, ORDER_LINE)
    {}

protected:
    virtual void GenerateRows(TContexts& ctxs, TGuard<TAdaptiveLock>&& g) override {
        TVector<order_t> ordersList(ctxs.front().GetCount());
        for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
            row_start(TableNum);
            mk_order(ctxs.front().GetStart() + i, &ordersList[i], 0);
            row_stop(TableNum);
        }
        g.Release();

        TCsvItemWriter<order_t> ordersWriter(ctxs[0].GetCsv().Out);
        CSV_WRITER_REGISTER_FIELD(ordersWriter, "o_orderkey", okey);
        CSV_WRITER_REGISTER_FIELD(ordersWriter, "o_custkey", custkey);
        ordersWriter.RegisterField("o_orderstatus", [](const decltype(ordersWriter)::TItem& item, IOutputStream& out) {
            out << TStringBuf(&item.orderstatus, 1);
        });
        CSV_WRITER_REGISTER_FIELD_MONEY(ordersWriter, "o_totalprice", totalprice);
        CSV_WRITER_REGISTER_FIELD_DATE(ordersWriter, "o_orderdate", odate);
        CSV_WRITER_REGISTER_FIELD(ordersWriter, "o_orderpriority", opriority);
        CSV_WRITER_REGISTER_FIELD(ordersWriter, "o_clerk", clerk);
        CSV_WRITER_REGISTER_FIELD(ordersWriter, "o_shippriority", spriority);
        CSV_WRITER_REGISTER_FIELD_COMMENT(ordersWriter, o);
        ordersWriter.Write(ordersList);

        TCsvItemWriter<line_t> linesWriter(ctxs[1].GetCsv().Out);
        CSV_WRITER_REGISTER_FIELD(linesWriter, "l_orderkey", okey);
        CSV_WRITER_REGISTER_FIELD(linesWriter, "l_partkey", partkey);
        CSV_WRITER_REGISTER_FIELD(linesWriter, "l_suppkey", suppkey);
        CSV_WRITER_REGISTER_FIELD(linesWriter, "l_linenumber", lcnt);
        CSV_WRITER_REGISTER_FIELD(linesWriter, "l_quantity", quantity);
        CSV_WRITER_REGISTER_FIELD_MONEY(linesWriter, "l_extendedprice", eprice);
        CSV_WRITER_REGISTER_FIELD_MONEY(linesWriter, "l_discount", discount);
        CSV_WRITER_REGISTER_FIELD_MONEY(linesWriter, "l_tax", tax);
        linesWriter.RegisterField("l_returnflag", [](const decltype(linesWriter)::TItem& item, IOutputStream& out) {
            out << TStringBuf(item.rflag, 1);
        });
        linesWriter.RegisterField("l_linestatus", [](const decltype(linesWriter)::TItem& item, IOutputStream& out) {
            out << TStringBuf(item.lstatus, 1);
        });
        CSV_WRITER_REGISTER_FIELD_DATE(linesWriter, "l_shipdate", sdate);
        CSV_WRITER_REGISTER_FIELD_DATE(linesWriter, "l_commitdate", cdate);
        CSV_WRITER_REGISTER_FIELD_DATE(linesWriter, "l_receiptdate", rdate);
        CSV_WRITER_REGISTER_FIELD(linesWriter, "l_shipinstruct", shipinstruct);
        CSV_WRITER_REGISTER_FIELD(linesWriter, "l_shipmode", shipmode);
        CSV_WRITER_REGISTER_FIELD_COMMENT(linesWriter, l);
        for (const auto& order: ordersList) {
            linesWriter.Write(order.l, order.lines);
        }
    };

    static const TFactory::TRegistrator<TBulkDataGeneratorOrderLine> Registrar;
};

const TTpchWorkloadDataInitializerGenerator::TBulkDataGenerator::TFactory::TRegistrator<TBulkDataGeneratorOrderLine> TBulkDataGeneratorOrderLine::Registrar("order_line");

class TBulkDataGeneratorPartPSupp : public TTpchWorkloadDataInitializerGenerator::TBulkDataGenerator {
public:
    explicit TBulkDataGeneratorPartPSupp(const TTpchWorkloadDataInitializerGenerator& owner)
        : TBulkDataGenerator(owner, PART_PSUPP)
    {}

protected:
    virtual void GenerateRows(TContexts& ctxs, TGuard<TAdaptiveLock>&& g) override {
        TVector<part_t> partsList(ctxs.front().GetCount());
        for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
            row_start(TableNum);
            mk_part(ctxs.front().GetStart() + i, &partsList[i]);
            row_stop(TableNum);
        }
        g.Release();

        TCsvItemWriter<part_t> partsWriter(ctxs[0].GetCsv().Out);
        CSV_WRITER_REGISTER_FIELD(partsWriter, "p_partkey", partkey);
        CSV_WRITER_REGISTER_FIELD(partsWriter, "p_name", name);
        CSV_WRITER_REGISTER_FIELD(partsWriter, "p_mfgr", mfgr);
        CSV_WRITER_REGISTER_FIELD(partsWriter, "p_brand", brand);
        CSV_WRITER_REGISTER_FIELD(partsWriter, "p_type", type);
        CSV_WRITER_REGISTER_FIELD(partsWriter, "p_size", size);
        CSV_WRITER_REGISTER_FIELD(partsWriter, "p_container", container);
        CSV_WRITER_REGISTER_FIELD_MONEY(partsWriter, "p_retailprice", retailprice);
        CSV_WRITER_REGISTER_FIELD_COMMENT(partsWriter, p);
        partsWriter.Write(partsList);

        TCsvItemWriter<partsupp_t> psuppsWriter(ctxs[1].GetCsv().Out);
        CSV_WRITER_REGISTER_FIELD(psuppsWriter, "ps_partkey", partkey);
        CSV_WRITER_REGISTER_FIELD(psuppsWriter, "ps_suppkey", suppkey);
        CSV_WRITER_REGISTER_FIELD(psuppsWriter, "ps_availqty", qty);
        CSV_WRITER_REGISTER_FIELD_MONEY(psuppsWriter, "ps_supplycost", scost);
        CSV_WRITER_REGISTER_FIELD_COMMENT(psuppsWriter, ps);
        for (const auto& part: partsList) {
            psuppsWriter.Write(part.s, SUPP_PER_PART);
        }
    };
    const static TFactory::TRegistrator<TBulkDataGeneratorPartPSupp> Registrar;
};

const TTpchWorkloadDataInitializerGenerator::TBulkDataGenerator::TFactory::TRegistrator<TBulkDataGeneratorPartPSupp> TBulkDataGeneratorPartPSupp::Registrar("part_psupp");

class TBulkDataGeneratorSupplier : public TTpchWorkloadDataInitializerGenerator::TBulkDataGenerator {
public:
    explicit TBulkDataGeneratorSupplier(const TTpchWorkloadDataInitializerGenerator& owner)
        : TBulkDataGenerator(owner, SUPP)
    {}

protected:
    virtual void GenerateRows(TContexts& ctxs, TGuard<TAdaptiveLock>&& g) override {
        TVector<supplier_t> suppList(ctxs.front().GetCount());
        for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
            row_start(TableNum);
            mk_supp(ctxs.front().GetStart() + i, &suppList[i]);
            row_stop(TableNum);
        }
        g.Release();

        TCsvItemWriter<supplier_t> writer(ctxs[0].GetCsv().Out);
        CSV_WRITER_REGISTER_FIELD(writer, "s_suppkey", suppkey);
        CSV_WRITER_REGISTER_FIELD(writer, "s_name", name);
        CSV_WRITER_REGISTER_FIELD(writer, "s_address", address);
        CSV_WRITER_REGISTER_FIELD(writer, "s_nationkey", nation_code);
        CSV_WRITER_REGISTER_FIELD(writer, "s_phone", phone);
        CSV_WRITER_REGISTER_FIELD_MONEY(writer, "s_acctbal", acctbal);
        CSV_WRITER_REGISTER_FIELD_COMMENT(writer, s);
        writer.Write(suppList);
    };
    const static TFactory::TRegistrator<TBulkDataGeneratorSupplier> Registrar;
};

const TTpchWorkloadDataInitializerGenerator::TBulkDataGenerator::TFactory::TRegistrator<TBulkDataGeneratorSupplier> TBulkDataGeneratorSupplier::Registrar("supplier");

class TBulkDataGeneratorCustomer : public TTpchWorkloadDataInitializerGenerator::TBulkDataGenerator {
public:
    explicit TBulkDataGeneratorCustomer(const TTpchWorkloadDataInitializerGenerator& owner)
        : TBulkDataGenerator(owner, CUST)
    {}

protected:
    virtual void GenerateRows(TContexts& ctxs, TGuard<TAdaptiveLock>&& g) override {
        TVector<customer_t> custList(ctxs.front().GetCount());
        for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
            row_start(TableNum);
            mk_cust(ctxs.front().GetStart() + i, &custList[i]);
            row_stop(TableNum);
        }
        g.Release();

        TCsvItemWriter<customer_t> writer(ctxs[0].GetCsv().Out);
        CSV_WRITER_REGISTER_FIELD(writer, "c_custkey", custkey);
        CSV_WRITER_REGISTER_FIELD(writer, "c_name", name);
        CSV_WRITER_REGISTER_FIELD(writer, "c_address", address);
        CSV_WRITER_REGISTER_FIELD(writer, "c_nationkey", nation_code);
        CSV_WRITER_REGISTER_FIELD(writer, "c_phone", phone);
        CSV_WRITER_REGISTER_FIELD_MONEY(writer, "c_acctbal", acctbal);
        CSV_WRITER_REGISTER_FIELD(writer, "c_mktsegment", mktsegment);
        CSV_WRITER_REGISTER_FIELD_COMMENT(writer, c);
        writer.Write(custList);
    };
    const static TFactory::TRegistrator<TBulkDataGeneratorCustomer> Registrar;
};

const TTpchWorkloadDataInitializerGenerator::TBulkDataGenerator::TFactory::TRegistrator<TBulkDataGeneratorCustomer> TBulkDataGeneratorCustomer::Registrar("customer");

class TBulkDataGeneratorNation : public TTpchWorkloadDataInitializerGenerator::TBulkDataGenerator {
public:
    explicit TBulkDataGeneratorNation(const TTpchWorkloadDataInitializerGenerator& owner)
        : TBulkDataGenerator(owner, NATION)
    {}

protected:
    virtual void GenerateRows(TContexts& ctxs, TGuard<TAdaptiveLock>&& g) override {
        TVector<code_t> nationList(ctxs.front().GetCount());
        for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
            row_start(TableNum);
            mk_nation(ctxs.front().GetStart() + i, &nationList[i]);
            row_stop(TableNum);
        }
        g.Release();

        TCsvItemWriter<code_t> writer(ctxs[0].GetCsv().Out);
        CSV_WRITER_REGISTER_FIELD(writer, "n_nationkey", code);
        CSV_WRITER_REGISTER_FIELD(writer, "n_name", text);
        CSV_WRITER_REGISTER_FIELD(writer, "n_regionkey", join);
        CSV_WRITER_REGISTER_FIELD_COMMENT(writer, n);
        writer.Write(nationList);
    };
    const static TFactory::TRegistrator<TBulkDataGeneratorNation> Registrar;
};

const TTpchWorkloadDataInitializerGenerator::TBulkDataGenerator::TFactory::TRegistrator<TBulkDataGeneratorNation> TBulkDataGeneratorNation::Registrar("nation");

class TBulkDataGeneratorRegion : public TTpchWorkloadDataInitializerGenerator::TBulkDataGenerator {
public:
    explicit TBulkDataGeneratorRegion(const TTpchWorkloadDataInitializerGenerator& owner)
        : TBulkDataGenerator(owner, REGION)
    {}

protected:
    virtual void GenerateRows(TContexts& ctxs, TGuard<TAdaptiveLock>&& g) override {
        TVector<code_t> regionList(ctxs.front().GetCount());
        for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
            row_start(TableNum);
            mk_region(ctxs.front().GetStart() + i, &regionList[i]);
            row_stop(TableNum);
        }
        g.Release();

        TCsvItemWriter<code_t> writer(ctxs[0].GetCsv().Out);
        CSV_WRITER_REGISTER_FIELD(writer, "r_regionkey", code);
        CSV_WRITER_REGISTER_FIELD(writer, "r_name", text);
        CSV_WRITER_REGISTER_FIELD_COMMENT(writer, r);
        writer.Write(regionList);
    };
    const static TFactory::TRegistrator<TBulkDataGeneratorRegion> Registrar;
};

const TTpchWorkloadDataInitializerGenerator::TBulkDataGenerator::TFactory::TRegistrator<TBulkDataGeneratorRegion> TBulkDataGeneratorRegion::Registrar("region");

}