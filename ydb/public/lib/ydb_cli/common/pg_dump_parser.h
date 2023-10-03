#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

#include <map>
#include <memory>
#include <functional>

namespace NYdb::NConsoleClient {

class TPgDumpParser {
    class TSQLCommandNode {
        using TSelfPtr = std::unique_ptr<TSQLCommandNode>;

        struct TEdge {
            TSelfPtr NodePtr;
            std::function<void()> Callback;
        };

    public:
        TSQLCommandNode* AddCommand(const TString& commandName, TSelfPtr node, const std::function<void()>& callback = []{});
        TSQLCommandNode* AddCommand(const TString& commandName, const std::function<void()>& callback = []{});
        TSQLCommandNode* AddOptionalCommand(const TString& commandName, const std::function<void()>& callback = []{});
        TSQLCommandNode* AddOptionalCommands(const TVector<TString>& commandNames);
        TSQLCommandNode* GetNextCommand(const TString& commandName);

    private:
        std::map<TString, TEdge> ChildNodes;
        std::map<TString, std::function<void()>> CycleCommands;
    };

public:
    explicit TPgDumpParser();

    void AddChar(char c);
    void WritePgDump(IOutputStream& stream);

private:
    static bool IsNewTokenSymbol(char c) {
        return c == '(' || c == ')' || c == ',' || c == ';' || c == '\'';
    }

    void FixPublicScheme();
    void ApplyToken();
    void EndToken();
    TString ExtractToken(TString* result, const std::function<bool(char)>& pred);
    void PgCatalogCheck();
    void AlterTableCheck();
    void CreateTableCheck();

    std::vector<TString> Buffers{""};
    std::map<TString, size_t> BufferIdByTableName;
    TString LastToken, TableName, PrimaryKeyName;
    bool IsCreateTable = false;
    bool IsWithStatement = false;
    bool IsSelect = false;
    bool IsAlterTable = false;
    bool IsCommentAlterTable = false;
    bool IsPrimaryKey = false;
    size_t BracesCount = 0;
    std::unique_ptr<TSQLCommandNode> Root = std::make_unique<TSQLCommandNode>();
    std::unique_ptr<TSQLCommandNode> WithParsingRoot = std::make_unique<TSQLCommandNode>();
    TSQLCommandNode* CurrentNode = Root.get();
};

} // NYdb::NConsoleClient
