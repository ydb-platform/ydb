
#include <ydb/library/yql/utils/backtrace/backtrace.h>

#include <ydb/library/yql/ast/yql_expr.h>

#include <library/cpp/svnversion/svnversion.h>

#include <util/stream/file.h>
#include <util/generic/yexception.h>

using namespace NYql;

int Main(int argc, const char *argv[])
{
    if (argc != 3) {
        PrintProgramSvnVersion();
        Cout << Endl << "Usage: " << argv[0] << " <fileone> <filetwo>" << Endl;
        return 2;
    }

    const TString fileOne(argv[1]), fileTwo(argv[2]);
    const auto progOne(ParseAst(TFileInput(fileOne).ReadAll())), progTwo(ParseAst(TFileInput(fileTwo).ReadAll()));

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
        Cerr << "Programs are not equal!" << Endl;
        if (rootOne->Type() != rootTwo->Type()) {
            Cerr << "Node in " << fileOne << " at [" << rootOnePos.Row << ":" << rootOnePos.Column << "] type is " << rootOne->Type() << Endl;
            Cerr << "Node in " << fileTwo << " at [" << rootTwoPos.Row << ":" << rootTwoPos.Column << "] type is " << rootTwo->Type() << Endl;
        } else if (rootOne->ChildrenSize() != rootTwo->ChildrenSize()) {
            Cerr << "Node '" << rootOne->Content() << "' in " << fileOne << " at [" << rootOnePos.Row << ":" << rootOnePos.Column << "] has " << rootOne->ChildrenSize() << " children." << Endl;
            Cerr << "Node '" << rootTwo->Content() << "' in " << fileTwo << " at [" << rootTwoPos.Row << ":" << rootTwoPos.Column << "] has " << rootTwo->ChildrenSize() << " children." << Endl;
        } else {
            Cerr << "Node in " << fileOne << " at [" << rootOnePos.Row << ":" << rootOnePos.Column << "]:" << Endl << rootOne->Dump() << Endl;
            Cerr << "Node in " << fileTwo << " at [" << rootTwoPos.Row << ":" << rootTwoPos.Column << "]:" << Endl << rootTwo->Dump() << Endl;
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
