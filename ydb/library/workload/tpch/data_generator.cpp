#include "data_generator.h"
#include "driver.h"
#include <util/folder/path.h>

namespace NYdbWorkload {

TTpchWorkloadDataInitializerGenerator::TTpchWorkloadDataInitializerGenerator(const TTpchWorkloadParams& params)
    : TWorkloadDataInitializerBase("generator", "Generate TPC-H dataset by native generator.", params)
{}

void TTpchWorkloadDataInitializerGenerator::ConfigureOpts(NLastGetopt::TOpts& opts) {
    TWorkloadDataInitializerBase::ConfigureOpts(opts);
    opts.AddLongOption("scale", "scale in percents")
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
    InitTpchGen(GetScale());
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


ui64 TTpchWorkloadDataInitializerGenerator::TBulkDataGenerator::CalcCountToGenerate(const TTpchWorkloadDataInitializerGenerator& owner, int tableNum, bool useState) {
    if (tableNum == NONE) {
        return 0;
    }
    ui64 position = 0;
    if (useState && owner.StateProcessor && owner.StateProcessor->GetState().contains(tdefs[tableNum].name)) {
        position = owner.StateProcessor->GetState().at(tdefs[tableNum].name).Position;
    }
    if (tableNum >= NATION) {
        return owner.GetProcessIndex() ? 0 : (tdefs[tableNum].base - position);
    }
    ui64 rowCount = tdefs[tableNum].base * owner.GetScale();
    ui64 extraRows = 0;
    if (owner.GetProcessIndex() + 1 >= owner.GetProcessCount()) {
        extraRows = rowCount % owner.GetProcessCount();
    }
    return rowCount / owner.GetProcessCount() + extraRows - position;
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
            Start - 1,
            Count
        ));
    } else if (Csv) {
        result.push_back(MakeIntrusive<TDataPortionWithState>(
            State,
            path,
            tdefs[TableNum].name,
            TDataPortion::TCsv(std::move(Csv), TWorkloadGeneratorBase::TsvFormatString),
            Start - 1,
            Count
        ));
    }
}

TString TTpchWorkloadDataInitializerGenerator::TBulkDataGenerator::GetFullTableName(const char* table) const {
    return Owner.Params.GetFullTableName(table);
}

TTpchWorkloadDataInitializerGenerator::TBulkDataGenerator::TBulkDataGenerator(const TTpchWorkloadDataInitializerGenerator& owner, int tableNum)
    : IBulkDataGenerator(tdefs[tableNum].name, CalcCountToGenerate(owner, tableNum, true))
    , TableNum(tableNum)
    , Owner(owner)
    , TableSize(CalcCountToGenerate(owner, tableNum, false))
{}

TTpchWorkloadDataInitializerGenerator::TBulkDataGenerator::TDataPortions TTpchWorkloadDataInitializerGenerator::TBulkDataGenerator::GenerateDataPortion() {
    TDataPortions result;
    if (TableSize == 0) {
        return result;
    }
    TContexts ctxs;
    ctxs.emplace_back(*this, TableNum, Owner.StateProcessor.Get());
    if (tdefs[TableNum].child != NONE) {
        ctxs.emplace_back(*this, tdefs[TableNum].child, nullptr);
    }
    with_lock(NumbersLock) {
        if (!Generated) {
            if (Owner.GetProcessCount() > 1) {
                DSS_HUGE e;
                set_state(TableNum, Owner.GetScale(), Owner.GetProcessCount(), Owner.GetProcessIndex() + 1, &e);
            }
            if (!!Owner.StateProcessor) {
                if (const auto* state = MapFindPtr(Owner.StateProcessor->GetState(), GetName())) {
                    Generated = state->Position;
                    GenSeed(TableNum, Generated);
                }
            }
        }
        const auto count = TableSize > Generated ? std::min(ui64(TableSize - Generated), Owner.Params.BulkSize) : 0;
        if (!count) {
            return result;
        }
        ctxs.front().SetCount(count);
        ctxs.front().SetStart((tdefs[TableNum].base * Owner.GetScale() / Owner.GetProcessCount()) * Owner.GetProcessIndex() + Generated + 1);
        Generated += count;
    }
    GenerateRows(ctxs);
    for(auto& ctx: ctxs) {
        ctx.AppendPortions(result);
    }
    return result;
}

class TBulkDataGeneratorOrderLine : public TTpchWorkloadDataInitializerGenerator::TBulkDataGenerator {
public:
    explicit TBulkDataGeneratorOrderLine(const TTpchWorkloadDataInitializerGenerator& owner)
        : TBulkDataGenerator(owner, ORDER_LINE)
    {}

protected:
    virtual void GenerateRows(TContexts& ctxs) override {
        TVector<order_t> orderList(ctxs.front().GetCount());
        with_lock(DriverLock) {
            for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
                mk_order(ctxs.front().GetStart() + i, &orderList[i], 0);
            }
        }
        auto& orders = ctxs[0].GetCsv();
        orders
            << "o_clerk" << TWorkloadGeneratorBase::TsvDelimiter
            << "o_comment" << TWorkloadGeneratorBase::TsvDelimiter
            << "o_custkey" << TWorkloadGeneratorBase::TsvDelimiter
            << "o_orderdate" << TWorkloadGeneratorBase::TsvDelimiter
            << "o_orderkey" << TWorkloadGeneratorBase::TsvDelimiter
            << "o_orderpriority" << TWorkloadGeneratorBase::TsvDelimiter
            << "o_orderstatus" << TWorkloadGeneratorBase::TsvDelimiter
            << "o_shippriority" << TWorkloadGeneratorBase::TsvDelimiter
            << "o_totalprice"
            << Endl;
        auto& lines = ctxs[1].GetCsv();
        lines
            << "l_comment" << TWorkloadGeneratorBase::TsvDelimiter
            << "l_commitdate" << TWorkloadGeneratorBase::TsvDelimiter
            << "l_discount" << TWorkloadGeneratorBase::TsvDelimiter
            << "l_extendedprice" << TWorkloadGeneratorBase::TsvDelimiter
            << "l_linenumber" << TWorkloadGeneratorBase::TsvDelimiter
            << "l_linestatus" << TWorkloadGeneratorBase::TsvDelimiter
            << "l_orderkey" << TWorkloadGeneratorBase::TsvDelimiter
            << "l_partkey" << TWorkloadGeneratorBase::TsvDelimiter
            << "l_quantity" << TWorkloadGeneratorBase::TsvDelimiter
            << "l_receiptdate" << TWorkloadGeneratorBase::TsvDelimiter
            << "l_returnflag" << TWorkloadGeneratorBase::TsvDelimiter
            << "l_shipdate" << TWorkloadGeneratorBase::TsvDelimiter
            << "l_shipinstruct" << TWorkloadGeneratorBase::TsvDelimiter
            << "l_shipmode" << TWorkloadGeneratorBase::TsvDelimiter
            << "l_suppkey" << TWorkloadGeneratorBase::TsvDelimiter
            << "l_tax"
            << Endl;
        for (const auto& order: orderList) {
            orders
                << order.clerk << TWorkloadGeneratorBase::TsvDelimiter
                << order.comment << TWorkloadGeneratorBase::TsvDelimiter
                << order.custkey << TWorkloadGeneratorBase::TsvDelimiter
                << ConvertDate(order.odate) << TWorkloadGeneratorBase::TsvDelimiter
                << order.okey << TWorkloadGeneratorBase::TsvDelimiter
                << order.opriority << TWorkloadGeneratorBase::TsvDelimiter
                << TStringBuf(&order.orderstatus, 1) << TWorkloadGeneratorBase::TsvDelimiter
                << order.spriority << TWorkloadGeneratorBase::TsvDelimiter
                << order.totalprice
                << Endl;
            for (i64 i = 0; i < order.lines; i++) {
                const auto& l = order.l[i];
                lines
                    << l.comment << TWorkloadGeneratorBase::TsvDelimiter
                    << ConvertDate(l.cdate) << TWorkloadGeneratorBase::TsvDelimiter
                    << l.discount << TWorkloadGeneratorBase::TsvDelimiter
                    << l.eprice << TWorkloadGeneratorBase::TsvDelimiter
                    << l.lcnt << TWorkloadGeneratorBase::TsvDelimiter
                    << TStringBuf(l.lstatus, 1) << TWorkloadGeneratorBase::TsvDelimiter
                    << l.okey << TWorkloadGeneratorBase::TsvDelimiter
                    << l.partkey << TWorkloadGeneratorBase::TsvDelimiter
                    << l.quantity << TWorkloadGeneratorBase::TsvDelimiter
                    << ConvertDate(l.rdate) << TWorkloadGeneratorBase::TsvDelimiter
                    << TStringBuf(l.rflag, 1) << TWorkloadGeneratorBase::TsvDelimiter
                    << ConvertDate(l.sdate) << TWorkloadGeneratorBase::TsvDelimiter
                    << l.shipinstruct << TWorkloadGeneratorBase::TsvDelimiter
                    << l.shipmode << TWorkloadGeneratorBase::TsvDelimiter
                    << l.suppkey << TWorkloadGeneratorBase::TsvDelimiter
                    << l.tax
                    << Endl;
            }
        }
    };

    static const TFactory::TRegistrator<TBulkDataGeneratorOrderLine> Registrar;

private:
    static ui64 ConvertDate(const char date[]) {
        return  TInstant::ParseIso8601(date).Days();
    }
};

const TTpchWorkloadDataInitializerGenerator::TBulkDataGenerator::TFactory::TRegistrator<TBulkDataGeneratorOrderLine> TBulkDataGeneratorOrderLine::Registrar("order_line");

class TBulkDataGeneratorOrderLineSdk : public TTpchWorkloadDataInitializerGenerator::TBulkDataGenerator {
public:
    explicit TBulkDataGeneratorOrderLineSdk(const TTpchWorkloadDataInitializerGenerator& owner)
        : TBulkDataGenerator(owner, ORDER_LINE)
    {}

protected:
    virtual void GenerateRows(TContexts& ctxs) override {
        TVector<order_t> orderList(ctxs.front().GetCount());
        with_lock(DriverLock) {
            for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
                mk_order(ctxs.front().GetStart() + i, &orderList[i], 0);
            }
        }
        auto& orders = ctxs[0].GetBuilder();
        auto& lines = ctxs[1].GetBuilder();
        for (const auto& order: orderList) {
            orders.AddListItem().BeginStruct()
                .AddMember("o_clerk").Utf8(order.clerk)
                .AddMember("o_comment").Utf8(order.comment)
                .AddMember("o_custkey").Int64(order.custkey)
                .AddMember("o_orderdate").Date(TInstant::ParseIso8601(order.odate))
                .AddMember("o_orderkey").Int64(order.okey)
                .AddMember("o_orderpriority").Utf8(order.opriority)
                .AddMember("o_orderstatus").Utf8(TString(order.orderstatus))
                .AddMember("o_shippriority").Int32(order.spriority)
                .AddMember("o_totalprice").Double(order.totalprice)
                .EndStruct();
            for (i64 i = 0; i < order.lines; i++) {
                const auto& l = order.l[i];
                lines.AddListItem().BeginStruct()
                    .AddMember("l_comment").Utf8(l.comment)
                    .AddMember("l_commitdate").Date(TInstant::ParseIso8601(l.cdate))
                    .AddMember("l_discount").Double(l.discount)
                    .AddMember("l_extendedprice").Double(l.eprice)
                    .AddMember("l_linenumber").Int32(l.lcnt)
                    .AddMember("l_linestatus").Utf8(l.lstatus)
                    .AddMember("l_orderkey").Int64(l.okey)
                    .AddMember("l_partkey").Int64(l.partkey)
                    .AddMember("l_quantity").Double(l.quantity)
                    .AddMember("l_receiptdate").Date(TInstant::ParseIso8601(l.rdate))
                    .AddMember("l_returnflag").Utf8(l.rflag)
                    .AddMember("l_shipdate").Date(TInstant::ParseIso8601(l.sdate))
                    .AddMember("l_shipinstruct").Utf8(l.shipinstruct)
                    .AddMember("l_shipmode").Utf8(l.shipmode)
                    .AddMember("l_suppkey").Int64(l.suppkey)
                    .AddMember("l_tax").Double(l.tax)
                    .EndStruct();
            }
        }
    };

    static const TFactory::TRegistrator<TBulkDataGeneratorOrderLineSdk> Registrar;
};

const TTpchWorkloadDataInitializerGenerator::TBulkDataGenerator::TFactory::TRegistrator<TBulkDataGeneratorOrderLineSdk> TBulkDataGeneratorOrderLineSdk::Registrar("__sdk_order_line");

class TBulkDataGeneratorPartPSupp : public TTpchWorkloadDataInitializerGenerator::TBulkDataGenerator {
public:
    explicit TBulkDataGeneratorPartPSupp(const TTpchWorkloadDataInitializerGenerator& owner)
        : TBulkDataGenerator(owner, PART_PSUPP)
    {}

protected:
    virtual void GenerateRows(TContexts& ctxs) override {
        TVector<part_t> partList(ctxs.front().GetCount());
        with_lock(DriverLock) {
            for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
                mk_part(ctxs.front().GetStart() + i, &partList[i]);
            }
        }
        auto& parts = ctxs.front().GetCsv();
        parts
            << "p_brand" << TWorkloadGeneratorBase::TsvDelimiter
            << "p_comment" << TWorkloadGeneratorBase::TsvDelimiter
            << "p_container" << TWorkloadGeneratorBase::TsvDelimiter
            << "p_mfgr" << TWorkloadGeneratorBase::TsvDelimiter
            << "p_name" << TWorkloadGeneratorBase::TsvDelimiter
            << "p_partkey" << TWorkloadGeneratorBase::TsvDelimiter
            << "p_retailprice" << TWorkloadGeneratorBase::TsvDelimiter
            << "p_size" << TWorkloadGeneratorBase::TsvDelimiter
            << "p_type"
            << Endl;
        auto& psupps = ctxs[1].GetCsv();
        psupps
            << "ps_availqty" << TWorkloadGeneratorBase::TsvDelimiter
            << "ps_comment" << TWorkloadGeneratorBase::TsvDelimiter
            << "ps_partkey" << TWorkloadGeneratorBase::TsvDelimiter
            << "ps_suppkey" << TWorkloadGeneratorBase::TsvDelimiter
            << "ps_supplycost"
            << Endl;

        for (const auto& part: partList) {
            parts
                << part.brand << TWorkloadGeneratorBase::TsvDelimiter
                << part.comment << TWorkloadGeneratorBase::TsvDelimiter
                << part.container << TWorkloadGeneratorBase::TsvDelimiter
                << part.mfgr << TWorkloadGeneratorBase::TsvDelimiter
                << part.name << TWorkloadGeneratorBase::TsvDelimiter
                << part.partkey << TWorkloadGeneratorBase::TsvDelimiter
                << part.retailprice << TWorkloadGeneratorBase::TsvDelimiter
                << part.size << TWorkloadGeneratorBase::TsvDelimiter
                << part.type
                << Endl;
            for (i64 i = 0; i < SUPP_PER_PART; i++) {
                const auto& s = part.s[i];
                psupps
                    << s.qty << TWorkloadGeneratorBase::TsvDelimiter
                    << s.comment << TWorkloadGeneratorBase::TsvDelimiter
                    << s.partkey << TWorkloadGeneratorBase::TsvDelimiter
                    << s.suppkey << TWorkloadGeneratorBase::TsvDelimiter
                    << s.scost
                    << Endl;
            }
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
    virtual void GenerateRows(TContexts& ctxs) override {
        TVector<supplier_t> suppList(ctxs.front().GetCount());
        with_lock(DriverLock) {
            for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
                mk_supp(ctxs.front().GetStart() + i, &suppList[i]);
            }
        }
        auto& suppliers = ctxs.front().GetCsv();
        suppliers
            << "s_acctbal" << TWorkloadGeneratorBase::TsvDelimiter
            << "s_address" << TWorkloadGeneratorBase::TsvDelimiter
            << "s_comment" << TWorkloadGeneratorBase::TsvDelimiter
            << "s_name" << TWorkloadGeneratorBase::TsvDelimiter
            << "s_nationkey" << TWorkloadGeneratorBase::TsvDelimiter
            << "s_phone" << TWorkloadGeneratorBase::TsvDelimiter
            << "s_suppkey"
            << Endl;
        for (const auto& supp: suppList) {
            suppliers
                << supp.acctbal << TWorkloadGeneratorBase::TsvDelimiter
                << supp.address << TWorkloadGeneratorBase::TsvDelimiter
                << supp.comment << TWorkloadGeneratorBase::TsvDelimiter
                << supp.name << TWorkloadGeneratorBase::TsvDelimiter
                << supp.nation_code << TWorkloadGeneratorBase::TsvDelimiter
                << supp.phone << TWorkloadGeneratorBase::TsvDelimiter
                << supp.suppkey
                << Endl;
        }
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
    virtual void GenerateRows(TContexts& ctxs) override {
        TVector<customer_t> custList(ctxs.front().GetCount());
        with_lock(DriverLock) {
            for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
                mk_cust(ctxs.front().GetStart() + i, &custList[i]);
            }
        }
        auto& customers = ctxs.front().GetCsv();
        customers
            << "c_acctbal" << TWorkloadGeneratorBase::TsvDelimiter
            << "c_address" << TWorkloadGeneratorBase::TsvDelimiter
            << "c_comment" << TWorkloadGeneratorBase::TsvDelimiter
            << "c_custkey" << TWorkloadGeneratorBase::TsvDelimiter
            << "c_mktsegment" << TWorkloadGeneratorBase::TsvDelimiter
            << "c_name" << TWorkloadGeneratorBase::TsvDelimiter
            << "c_nationkey" << TWorkloadGeneratorBase::TsvDelimiter
            << "c_phone"
            << Endl;
        for (const auto& cust: custList) {
            customers
                << cust.acctbal << TWorkloadGeneratorBase::TsvDelimiter
                << cust.address << TWorkloadGeneratorBase::TsvDelimiter
                << cust.comment << TWorkloadGeneratorBase::TsvDelimiter
                << cust.custkey << TWorkloadGeneratorBase::TsvDelimiter
                << cust.mktsegment << TWorkloadGeneratorBase::TsvDelimiter
                << cust.name << TWorkloadGeneratorBase::TsvDelimiter
                << cust.nation_code << TWorkloadGeneratorBase::TsvDelimiter
                << cust.phone
                << Endl;
        }
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
    virtual void GenerateRows(TContexts& ctxs) override {
        TVector<code_t> nationList(ctxs.front().GetCount());
        with_lock(DriverLock) {
            for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
                mk_nation(ctxs.front().GetStart() + i, &nationList[i]);
            }
        }
        auto& nations = ctxs.front().GetCsv();
        nations
            << "n_comment" << TWorkloadGeneratorBase::TsvDelimiter
            << "n_name" << TWorkloadGeneratorBase::TsvDelimiter
            << "n_nationkey" << TWorkloadGeneratorBase::TsvDelimiter
            << "n_regionkey"
            << Endl;
        for (const auto& nation: nationList) {
            nations
                << TString(nation.comment, nation.clen) << TWorkloadGeneratorBase::TsvDelimiter
                << nation.text << TWorkloadGeneratorBase::TsvDelimiter
                << nation.code << TWorkloadGeneratorBase::TsvDelimiter
                << nation.join
                << Endl;
        }
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
    virtual void GenerateRows(TContexts& ctxs) override {
        TVector<code_t> regionList(ctxs.front().GetCount());
        with_lock(DriverLock) {
            for (ui64 i = 0; i < ctxs.front().GetCount(); ++i) {
                mk_region(ctxs.front().GetStart() + i, &regionList[i]);
            }
        }
        auto& regions = ctxs.front().GetCsv();
        regions
            << "r_comment" << TWorkloadGeneratorBase::TsvDelimiter
            << "r_name" << TWorkloadGeneratorBase::TsvDelimiter
            << "r_regionkey"
            << Endl;
        for (const auto & region: regionList) {
            regions
                << TString(region.comment, region.clen) << TWorkloadGeneratorBase::TsvDelimiter
                << region.text << TWorkloadGeneratorBase::TsvDelimiter
                << region.code
                << Endl;
        }
    };
    const static TFactory::TRegistrator<TBulkDataGeneratorRegion> Registrar;
};

const TTpchWorkloadDataInitializerGenerator::TBulkDataGenerator::TFactory::TRegistrator<TBulkDataGeneratorRegion> TBulkDataGeneratorRegion::Registrar("region");

}