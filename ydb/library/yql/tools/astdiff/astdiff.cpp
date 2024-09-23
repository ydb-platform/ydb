
#include <ydb/library/yql/utils/backtrace/backtrace.h>

#include <ydb/library/yql/ast/yql_expr.h>

#include <library/cpp/svnversion/svnversion.h>

#include <util/stream/file.h>
#include <util/folder/path.h>
#include <util/string/split.h>
#include <util/generic/yexception.h>

#include <sstream>

#include <contrib/libs/dtl/dtl/dtl.hpp>

using namespace NYql;

std::string CalculateDiff(const TString& oldAst, const TString& newAst) {
    auto oldLines = StringSplitter(oldAst).Split('\n').ToList<std::string>();
    auto newLines = StringSplitter(newAst).Split('\n').ToList<std::string>();

    dtl::Diff<std::string, TVector<std::string>> d(oldLines, newLines);
    d.compose();
    d.composeUnifiedHunks();
    
    std::ostringstream ss;
    d.printUnifiedFormat(ss);
    return ss.str();
}


const int DIFF_LINES_LIMIT = 16;

void DumpSmallNodes(const TExprNode* rootOne, const TExprNode* rootTwo) {
    const auto isDumpSmall = [] (const TString& dump) {
        return std::count(dump.begin(), dump.end(), '\n') < DIFF_LINES_LIMIT;
    };
    const auto rootOneDump = rootOne->Dump();
    if (!isDumpSmall(rootOneDump)) {
        return;
    }
    const auto rootTwoDump = rootTwo->Dump();
    if (!isDumpSmall(rootTwoDump)) {
        return;
    }

    Cerr << rootOneDump << '\n' << rootTwoDump;
}

int Main(int argc, const char *argv[])
{
    if (argc != 3) {
        PrintProgramSvnVersion();
        Cout << Endl << "Usage: " << argv[0] << " <fileone> <filetwo>" << Endl;
        return 2;
    }

    const TString fileOne(argv[1]), fileTwo(argv[2]);
    const TString progOneAst = TFileInput(fileOne).ReadAll();
    const TString progTwoAst = TFileInput(fileTwo).ReadAll();
    const auto progOne(ParseAst(progOneAst)), progTwo(ParseAst(progTwoAst));

    if (!(progOne.IsOk() && progTwo.IsOk())) {
        if (!progOne.IsOk()) {
            Cerr << "Errors in " << fileOne << Endl;
            progOne.Issues.PrintTo(Cerr);
        }
        if (!progTwo.IsOk()) {
            Cerr << "Errors in " << fileTwo << Endl;
            progTwo.Issues.PrintTo(Cerr);
        }
        return 3;
    }

    TExprContext ctxOne, ctxTwo;
    TExprNode::TPtr exprOne, exprTwo;

    const bool okOne = CompileExpr(*progOne.Root, exprOne, ctxOne, nullptr, nullptr);
    const bool okTwo = CompileExpr(*progTwo.Root, exprTwo, ctxTwo, nullptr, nullptr);

    if (!(okOne && okTwo)) {
        if (!okOne) {
            Cerr << "Errors on compile " << fileOne << Endl;
            ctxOne.IssueManager.GetIssues().PrintTo(Cerr);
        }
        if (!okTwo) {
            Cerr << "Errors on compile " << fileTwo << Endl;
            ctxTwo.IssueManager.GetIssues().PrintTo(Cerr);
        }
        return 4;
    }

    const TExprNode* rootOne = exprOne.Get();
    const TExprNode* rootTwo = exprTwo.Get();

    auto rootOnePos = ctxOne.GetPosition(rootOne->Pos());
    auto rootTwoPos = ctxTwo.GetPosition(rootTwo->Pos());
    if (!CompareExprTrees(rootOne, rootTwo)) {
        const auto diff = CalculateDiff(progOneAst, progTwoAst);

        Cerr << "Programs are not equal!" << Endl;
        if (rootOne->Type() != rootTwo->Type()) {
            Cerr << "Node in " << fileOne << " at [" << rootOnePos.Row << ":" << rootOnePos.Column << "] type is " << rootOne->Type() << Endl;
            Cerr << "Node in " << fileTwo << " at [" << rootTwoPos.Row << ":" << rootTwoPos.Column << "] type is " << rootTwo->Type() << Endl;
            Cerr << "\nFile diff:\n" << diff;
        } else if (rootOne->ChildrenSize() != rootTwo->ChildrenSize()) {
            Cerr << "Node '" << rootOne->Content() << "' in " << fileOne << " at [" << rootOnePos.Row << ":" << rootOnePos.Column << "] has " << rootOne->ChildrenSize() << " children." << Endl;
            Cerr << "Node '" << rootTwo->Content() << "' in " << fileTwo << " at [" << rootTwoPos.Row << ":" << rootTwoPos.Column << "] has " << rootTwo->ChildrenSize() << " children." << Endl;
            DumpSmallNodes(rootOne, rootTwo);
            Cerr << "\nFile diff:\n" << diff;
        } else {
            Cerr << "Node in " << fileOne << " at [" << rootOnePos.Row << ":" << rootOnePos.Column << "]:";
            Cerr << "Node in " << fileTwo << " at [" << rootTwoPos.Row << ":" << rootTwoPos.Column << "]:";
            DumpSmallNodes(rootOne, rootTwo);
            Cerr << "\nFile diff:\n" << diff;
        }
        return 5;
    }

    return 0;
}

int main(int argc, const char *argv[]) {
    NYql::NBacktrace::RegisterKikimrFatalActions();
    NYql::NBacktrace::EnableKikimrSymbolize();

    try {
        return Main(argc, argv);
    }
    catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
