
#include <ydb/library/yql/utils/backtrace/backtrace.h>

#include <ydb/library/yql/ast/yql_expr.h>

#include <library/cpp/svnversion/svnversion.h>

#include <util/stream/file.h>
#include <util/system/shellcommand.h>
#include <util/folder/tempdir.h>
#include <util/folder/path.h>
#include <util/generic/yexception.h>

#include <sstream>

#include <contrib/libs/dtl/dtl/dtl.hpp>

using namespace NYql;

std::string CalculateDiff(const TFsPath& oldPath, const TFsPath& newPath) {
    TFileInput oldIn(oldPath);
    TFileInput newIn(newPath);

    std::vector<std::string> oldLines;
    std::vector<std::string> newLines;
    TString line;
    while (oldIn.ReadLine(line)) {
        oldLines.push_back(std::move(line));
        line = {};
    }
    while (newIn.ReadLine(line)) {
        newLines.push_back(std::move(line));
        line = {};
    }

    dtl::Diff<std::string, std::vector<std::string>> d(oldLines, newLines);
    d.compose();
    d.composeUnifiedHunks();
    
    std::ostringstream ss;
    d.printUnifiedFormat(ss);
    return ss.str();
}

std::pair<TFsPath, TFsPath> CopyFilesToTemp(const TFsPath& oldAstPath, const TFsPath& newAstPath) {
    const auto tempPath = TFsPath("/tmp") / "astdiff";
    tempPath.MkDir();

    const auto oldAstTempPath = tempPath / "old.yql";
    const auto newAstTempPath = tempPath / "new.yql";
    oldAstPath.CopyTo(oldAstTempPath, /* force */ true);
    newAstPath.CopyTo(newAstTempPath, /* force */ true);
    return {oldAstTempPath, newAstTempPath};
}

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
        const auto [oldAstTempPath, newAstTempPath] = CopyFilesToTemp(fileOne, fileTwo);
        const auto diff = CalculateDiff(oldAstTempPath, newAstTempPath);

        Cerr << "Programs are not equal!" << Endl;
        if (rootOne->Type() != rootTwo->Type()) {
            Cerr << "Node in " << fileOne << " at [" << rootOnePos.Row << ":" << rootOnePos.Column << "] type is " << rootOne->Type() << Endl;
            Cerr << "Node in " << fileTwo << " at [" << rootTwoPos.Row << ":" << rootTwoPos.Column << "] type is " << rootTwo->Type() << Endl;
            Cerr << "\nFile diff:\n" << diff;
        } else if (rootOne->ChildrenSize() != rootTwo->ChildrenSize()) {
            Cerr << "Node '" << rootOne->Content() << "' in " << oldAstTempPath << " at [" << rootOnePos.Row << ":" << rootOnePos.Column << "] has " << rootOne->ChildrenSize() << " children." << Endl;
            Cerr << rootOne->Dump();
            Cerr << "Node '" << rootTwo->Content() << "' in " << newAstTempPath << " at [" << rootTwoPos.Row << ":" << rootTwoPos.Column << "] has " << rootTwo->ChildrenSize() << " children." << Endl;
            Cerr << rootTwo->Dump();
            Cerr << "\nFile diff:\n" << diff;
        } else {
            Cerr << "Node in " << oldAstTempPath << " at [" << rootOnePos.Row << ":" << rootOnePos.Column << "]:" << Endl << rootOne->Dump() << Endl;
            Cerr << "Node in " << newAstTempPath << " at [" << rootTwoPos.Row << ":" << rootTwoPos.Column << "]:" << Endl << rootTwo->Dump() << Endl;
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