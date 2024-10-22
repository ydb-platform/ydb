#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

#include <map>
#include <memory>
#include <functional>

namespace NYdb::NConsoleClient {

class TFixedStringStream : public IInputStream {
public:
    TFixedStringStream(const TString& data) 
        : Data(data)
    {}

    TFixedStringStream(TString&& data) 
        : Data(std::move(data))
    {}

    void MovePointer(size_t position = 0) {
        Position = position;
    }

protected:
    virtual size_t DoRead(void* buf, size_t len) override;
    virtual size_t DoSkip(size_t len) override;
    virtual size_t DoReadTo(TString& st, char ch) override;
    virtual ui64 DoReadAll(IOutputStream& out) override;

private:
    TString Data;
    size_t Position = 0;
};

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
    explicit TPgDumpParser(IOutputStream& out, bool ignoreUnsupported);

    void Prepare(IInputStream& in);
    void WritePgDump(IInputStream& in);

private:
    static bool IsPrimaryKeyTokens(const TVector<TString>& tokens);
    static bool IsNewTokenSymbol(char c) {
        return c == '(' || c == ')' || c == ',' || c == ';' || c == '\'';
    }

    void ReadStream(IInputStream& in, bool isPrepare);
    void ApplyToken(TSQLCommandNode* root);
    void EndToken(bool isPrepare);
    TString ExtractToken(TString* result, const std::function<bool(char)>& pred);

    void FixPublicScheme();
    void PgCatalogCheck();
    void AlterTableCheck();
    void CreateTableCheck();
    void PrimaryKeyCheck();

    TString Buffer, LastTokenBuffer;
    std::map<TString, TString> PrimaryKeyByTable;
    TString LastToken, TableName, PrimaryKeyName;
    bool IsCreateTable = false;
    bool IsWithStatement = false;
    bool IsSelect = false;
    bool IsAlterTable = false;
    bool IsPrimaryKey = false;
    bool IsCommented = false;
    bool NotFlush = false;

    size_t BracesCount = 0;

    IOutputStream& Out;
    const bool IgnoreUnsupported;

    TSQLCommandNode RunRoot, PrepareRoot;
    TSQLCommandNode* CurrentNode = nullptr;
};

} // NYdb::NConsoleClient
