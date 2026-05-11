// Read-only inspector for schemeshard operation handler registration.

#include <ydb/tools/ss_tool/lib/op_inspect.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/json_value.h>

#include <util/generic/string.h>
#include <util/stream/output.h>

namespace {

using namespace NKikimr::NSchemeShard;
using namespace NKikimr::NSchemeShard::NSsTool;
namespace NSO = NKikimrSchemeOp;

void PrintHeader() {
    Cout << "NAME\tNUMBER\tREGISTERED" << Endl;
}

void PrintRow(const TOpRow& op) {
    Cout << op.Name
         << "\t" << static_cast<int>(op.Type)
         << "\t" << (op.IsRegistered ? "yes" : "no")
         << Endl;
}

int CmdList(int argc, const char** argv) {
    bool registeredOnly = false;
    bool unregisteredOnly = false;
    TString format;

    NLastGetopt::TOpts opts;
    opts.AddLongOption("registered", "only show ops listed in op_handler_overrides.yaml")
        .NoArgument().SetFlag(&registeredOnly);
    opts.AddLongOption("unregistered", "only show ops still using the legacy switch")
        .NoArgument().SetFlag(&unregisteredOnly);
    opts.AddLongOption("format", "output format: tsv (default) or json")
        .StoreResult(&format);
    opts.AddHelpOption();
    NLastGetopt::TOptsParseResult res(&opts, argc, argv);

    TVector<TOpRow> filtered;
    for (const auto& op : AllOps()) {
        if (registeredOnly && !op.IsRegistered) continue;
        if (unregisteredOnly && op.IsRegistered) continue;
        filtered.push_back(op);
    }

    if (format == "json") {
        NJson::TJsonValue root(NJson::JSON_ARRAY);
        for (const auto& op : filtered) {
            NJson::TJsonValue entry(NJson::JSON_MAP);
            entry.InsertValue("name", op.Name);
            entry.InsertValue("number", static_cast<int>(op.Type));
            entry.InsertValue("registered", op.IsRegistered);
            root.AppendValue(std::move(entry));
        }
        NJson::WriteJson(&Cout, &root, /*formatOutput=*/true);
        Cout << Endl;
    } else {
        PrintHeader();
        for (const auto& op : filtered) {
            PrintRow(op);
        }
    }
    return 0;
}

int CmdShow(int argc, const char** argv) {
    if (argc < 2) {
        Cerr << "Usage: ss_tool ops show <OpName>" << Endl;
        return 1;
    }
    const auto* d = NSO::EOperationType_descriptor();
    const auto* v = d->FindValueByName(argv[1]);
    if (!v) {
        Cerr << "Unknown op: " << argv[1] << Endl;
        return 1;
    }
    const auto row = CollectRow(static_cast<NSO::EOperationType>(v->number()));
    Cout << "Name:        " << row.Name << Endl;
    Cout << "Number:      " << static_cast<int>(row.Type) << Endl;
    Cout << "Registered:  " << (row.IsRegistered ? "yes" : "no") << Endl;
    return 0;
}

int CmdMigrationStatus(int /*argc*/, const char** /*argv*/) {
    const auto ops = AllOps();
    size_t registered = 0;
    for (const auto& op : ops) {
        if (op.IsRegistered) ++registered;
    }
    Cout << "Total ops:     " << ops.size() << Endl;
    Cout << "Registered:    " << registered << Endl;
    Cout << "Pending:       " << (ops.size() - registered) << Endl;
    Cout << Endl;
    Cout << "Pending ops (still routed through the legacy central switches):" << Endl;
    for (const auto& op : ops) {
        if (!op.IsRegistered) {
            Cout << "  " << op.Name << Endl;
        }
    }
    return 0;
}

void PrintUsage(const char* argv0) {
    Cerr << "Usage: " << argv0 << " ops <subcommand> [args...]" << Endl;
    Cerr << "Subcommands:" << Endl;
    Cerr << "  list [--registered] [--unregistered]" << Endl;
    Cerr << "  show <OpName>" << Endl;
    Cerr << "  migration-status" << Endl;
}

} // namespace

int main(int argc, const char** argv) {
    if (argc < 3 || TString(argv[1]) != "ops") {
        PrintUsage(argv[0]);
        return 1;
    }
    const TString sub = argv[2];
    if (sub == "list")             return CmdList(argc - 2, argv + 2);
    if (sub == "show")             return CmdShow(argc - 2, argv + 2);
    if (sub == "migration-status") return CmdMigrationStatus(argc - 2, argv + 2);
    PrintUsage(argv[0]);
    return 1;
}
