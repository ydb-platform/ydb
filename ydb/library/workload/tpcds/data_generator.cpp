#include "data_generator.h"
#include <ydb/public/api/protos/ydb_formats.pb.h>
#include <library/cpp/charset/wide.h>
#include <util/string/escape.h>


extern "C" {
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/address.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/date.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/decimal.h>
    #include <ydb/library/benchmarks/gen/tpcds-dbgen/parallel.h>
}

namespace NYdbWorkload {

TTpcdsWorkloadDataInitializerGenerator::TTpcdsWorkloadDataInitializerGenerator(const TTpcdsWorkloadParams& params)
    : TWorkloadDataInitializerBase("generator", "Generate TPC-DS dataset by native generator.", params)
{}

void TTpcdsWorkloadDataInitializerGenerator::ConfigureOpts(NLastGetopt::TOpts& opts) {
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

TBulkDataGeneratorList TTpcdsWorkloadDataInitializerGenerator::DoGetBulkInitialData() {
    InitTpcdsGen(GetScale(), GetProcessCount(), GetProcessIndex());
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

TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::TPositions TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::CalcCountToGenerate(const TTpcdsWorkloadDataInitializerGenerator& owner, int tableNum, bool useState) {
    static const TSet<ui32> allowedModules{1, 2, 4};
    TPositions result;
    const auto* tdef = getTdefsByNumber(tableNum);
    if (!tdef) {
        return result;
    }
    split_work(tableNum, &result.FirstRow, &result.Count);
    if (useState && owner.StateProcessor && owner.StateProcessor->GetState().contains(tdef->name)) {
        result.Position = owner.StateProcessor->GetState().at(tdef->name).Position;

        //this magic is needed for SCD to work correctly. See setSCDKeys in ydb/library/benchmarks/gen/tpcds-dbgen/scd.c
        while (result.Position && !allowedModules.contains((result.FirstRow + result.Position) % 6)) {
            --result.Position;
        }
    }
    //this magic is needed for SCD to work correctly. See setSCDKeys in ydb/library/benchmarks/gen/tpcds-dbgen/scd.c
    while (result.FirstRow > 1 && !allowedModules.contains((result.FirstRow + result.Position) % 6)) {
        --result.FirstRow;
        ++result.Count;
    }
    return result;
}

TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::TContext::TContext(const TBulkDataGenerator& owner, int tableNum)
    : Owner(owner)
    , TableNum(tableNum)
{}

NYdb::TValueBuilder& TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::TContext::GetBuilder() {
    if (!Builder) {
        Builder = MakeHolder<NYdb::TValueBuilder>();
        Builder->BeginList();
    }
    return *Builder;
}

TStringBuilder& TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::TContext::GetCsv() {
    return Csv;
}

void TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::TContext::AppendPortions(TDataPortions& result) {
    const auto name = getTdefsByNumber(TableNum)->name;
    const auto path = Owner.GetFullTableName(name);
    if (Builder) {
        Builder->EndList();
        result.push_back(MakeIntrusive<TDataPortionWithState>(
            Owner.Owner.StateProcessor.Get(),
            path,
            name,
            Builder->Build(),
            Start - 1,
            Count
        ));
    } else if (Csv) {
        result.push_back(MakeIntrusive<TDataPortionWithState>(
            Owner.Owner.StateProcessor.Get(),
            path,
            name,
            TDataPortion::TCsv(std::move(Csv), TWorkloadGeneratorBase::PsvFormatString),
            Start - 1,
            Count
        ));
    }
}

TString TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::GetFullTableName(const char* table) const {
    return Owner.Params.GetFullTableName(table);
}

TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::TBulkDataGenerator(const TTpcdsWorkloadDataInitializerGenerator& owner, int tableNum)
    : IBulkDataGenerator(getTdefsByNumber(tableNum)->name, CalcCountToGenerate(owner, tableNum, true).Count)
    , TableNum(tableNum)
    , Owner(owner)
{}

TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::TDataPortions TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::GenerateDataPortion() {
    TDataPortions result;
    if (GetSize() == 0) {
        return result;
    }
    TContexts ctxs;
    ctxs.emplace_back(*this, TableNum);

    const auto* tdef = getTdefsByNumber(TableNum);
    if (tdef->flags & FL_PARENT) {
        ctxs.emplace_back(*this, tdef->nParam);
    }

    auto g = Guard(Lock);
    auto positions = CalcCountToGenerate(Owner, TableNum, !Generated);
    if (!Generated) {
        Generated = positions.Position;
        result.push_back(MakeIntrusive<TDataPortion>(
            GetFullTableName(tdef->name),
            TDataPortion::TSkip(),
            Generated
        ));
        if (const ui32 toSkip = positions.FirstRow + positions.Position - 1) {
            row_skip(TableNum, toSkip);
            if (tdef->flags & FL_PARENT) {
                row_skip(tdef->nParam, toSkip);
            }
        }
        if (tdef->flags & FL_SMALL) {
            resetCountCount();
        }
    }
    const auto count = GetSize() > Generated ? std::min(ui64(GetSize() - Generated), Owner.Params.BulkSize) : 0;
    if (!count) {
        return result;
    }
    ctxs.front().SetCount(count);
    ctxs.front().SetStart(positions.FirstRow + Generated);
    Generated += count;
    GenerateRows(ctxs, std::move(g));
    for(auto& ctx: ctxs) {
        ctx.AppendPortions(result);
    }
    return result;
}

TString TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::ConvertString(const char str[]) {
    return  WideToUTF8(CharToWide(str, CODES_ISO_8859_15));
}

TString TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::ConvertDate(i64 val) {
    if (val > 0) {
        DATE_T date;
        jtodt(&date, val);
        return Sprintf("%4d-%02d-%02d", date.year, date.month, date.day);
    }
    return TString();
}

TString TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator::ConvertDecimal(const DECIMAL_T& val) {
        double dTemp = val.number;
        for (int i = 0; i < val.precision; i++) {
            dTemp /= 10.0;
        }
        return Sprintf("%.*f", val.precision, dTemp);
}

}