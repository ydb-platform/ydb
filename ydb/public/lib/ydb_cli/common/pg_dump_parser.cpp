#include "pg_dump_parser.h"

#include <ydb/public/lib/ydb_cli/common/common.h>

namespace NYdb::NConsoleClient {

size_t TFixedStringStream::DoRead(void* buf, size_t len) {
    len = std::min(len, Data.Size() - Position);
    memcpy(buf, Data.data() + Position, len);
    Position += len;
    return len;
}

size_t TFixedStringStream::DoSkip(size_t len) {
    len = std::min(len, Data.Size() - Position);
    Position += len;
    return len;
}

size_t TFixedStringStream::DoReadTo(TString& st, char ch) {
    size_t len = std::min(Data.find_first_of(ch, Position), Data.Size()) - Position;
    st += Data.substr(Position, len);
    Position += len;
    return len;
}

ui64 TFixedStringStream::DoReadAll(IOutputStream& out) {
    out << Data.substr(Position);
    size_t len = Data.Size() - Position;
    Position = Data.Size();
    return len;
}

TPgDumpParser::TSQLCommandNode* TPgDumpParser::TSQLCommandNode::AddCommand(const TString& commandName,
        TSelfPtr node, const std::function<void()>& callback) {
    ChildNodes.insert({commandName, TEdge{std::move(node), callback}});
    return ChildNodes.at(commandName).NodePtr.get();
}

TPgDumpParser::TSQLCommandNode* TPgDumpParser::TSQLCommandNode::AddCommand(const TString& commandName,
        const std::function<void()>& callback) {
    ChildNodes.insert({commandName, TEdge{std::make_unique<TSQLCommandNode>(), callback}});
    return ChildNodes.at(commandName).NodePtr.get();
}

TPgDumpParser::TSQLCommandNode* TPgDumpParser::TSQLCommandNode::AddOptionalCommand(const TString& commandName,
        const std::function<void()>& callback) {
    CycleCommands.insert({commandName, callback});
    return this;
}

TPgDumpParser::TSQLCommandNode* TPgDumpParser::TSQLCommandNode::AddOptionalCommands(const TVector<TString>& commandNames) {
    for (const auto& name : commandNames) {
        CycleCommands.insert({name, []{}});
    }
    return this;
}

TPgDumpParser::TSQLCommandNode* TPgDumpParser::TSQLCommandNode::GetNextCommand(const TString& commandName) {
    if (ChildNodes.find(commandName) != ChildNodes.end()) {
        ChildNodes.at(commandName).Callback();
        return ChildNodes.at(commandName).NodePtr.get();
    }
    if (CycleCommands.find(commandName) != CycleCommands.end()) {
        CycleCommands.at(commandName)();
        return this;
    }
    return nullptr;
}

TPgDumpParser::TPgDumpParser(IOutputStream& out, bool ignoreUnsupported) : Out(out), IgnoreUnsupported(ignoreUnsupported) {
    auto saveTableName = [this] {
        FixPublicScheme();
        TableName = LastToken;
    };
    auto searchPrimaryKeyNode = RunRoot.AddCommand("CREATE")->
        AddOptionalCommands({"GLOBAL", "LOCAL", "TEMP", "TEMPORARY", "UNLOGGED"})->
        AddCommand("TABLE")->AddOptionalCommands({"IF", "NOT", "EXISTS"})->
        AddCommand("", saveTableName);
    searchPrimaryKeyNode->AddCommand("PRIMARY")->AddCommand("KEY", [this] {
        IsCreateTable = false; 
        BracesCount = 0;
    });
    searchPrimaryKeyNode->AddOptionalCommand("(", [this]{++BracesCount;});
    searchPrimaryKeyNode->AddOptionalCommand(")", [this]{--BracesCount;});
    searchPrimaryKeyNode->AddOptionalCommand("");
    searchPrimaryKeyNode->AddCommand(";", [this]{NotFlush = true;});
    searchPrimaryKeyNode->AddCommand("WITH")->AddCommand("(")->AddOptionalCommand("")->AddCommand(")", [this]{IsWithStatement = true;});

    RunRoot.AddCommand("INSERT")->AddCommand("INTO")->AddCommand("", saveTableName);
    RunRoot.AddCommand("SELECT")->AddCommand("", saveTableName)->AddOptionalCommand("")->AddCommand(";", [this]{IsSelect = true; NotFlush = true;});
    RunRoot.AddCommand("ALTER")->AddCommand("TABLE")->AddOptionalCommand("")->AddCommand(";", [this]{IsAlterTable=true; NotFlush = true;});
    RunRoot.AddCommand("--", [this]{IsCommented = true; NotFlush = true;});
    PrepareRoot.AddCommand("ALTER")->AddCommand("TABLE")->AddCommand("ONLY")->
        AddCommand("", saveTableName)->AddCommand("ADD")->AddCommand("CONSTRAINT")->
        AddCommand("")->AddCommand("PRIMARY")->AddCommand("KEY")->
        AddCommand("(")->AddCommand("", [this]{PrimaryKeyName=LastToken;})->
        AddCommand(")")->AddCommand(";", [this]{IsPrimaryKey=true; NotFlush = true;});
}

void TPgDumpParser::ReadStream(IInputStream& in, bool isPrepare) {
    char c;
    while (in.ReadChar(c)) {
        if (!LastToken.empty() && (std::isspace(c) || IsNewTokenSymbol(c) || IsNewTokenSymbol(Buffer.back()))) {
            EndToken(isPrepare);
        }
        if (c == '\n') {
            IsCommented = false;
        }
        if (!std::isspace(c)) {
            LastToken += c;
        }
        Buffer += c;
    }
    EndToken(isPrepare);
}

void TPgDumpParser::Prepare(IInputStream& in) {
    CurrentNode = &PrepareRoot;
    ReadStream(in, true);
    Buffer.clear();
}

void TPgDumpParser::WritePgDump(IInputStream& in) {
    CurrentNode = &RunRoot;
    ReadStream(in, false);
    Out << Buffer;
    Buffer.clear();
}

void TPgDumpParser::FixPublicScheme() {
    if (LastToken.StartsWith("public.")) {
        Buffer.remove(Buffer.size() - LastToken.size());
        TStringBuf token = LastToken;
        token.remove_prefix(TString("public.").size());
        Buffer += token;
    }
}

void TPgDumpParser::ApplyToken(TSQLCommandNode* root) {
    auto next = CurrentNode->GetNextCommand(LastToken);
    if (next != nullptr) {
        CurrentNode = next;
        return;
    }
    next = CurrentNode->GetNextCommand("");
    if (next != nullptr) {
        CurrentNode = next;
        return;
    }
    if (CurrentNode != root) {
        CurrentNode = root;
        if (!NotFlush) {
            if (root != &PrepareRoot) {
                Out << Buffer;
            }
            Buffer.clear();
        }
        NotFlush = false;
        ApplyToken(root);
    }
}

void TPgDumpParser::PrimaryKeyCheck() {
    if (IsPrimaryKey) {
        PrimaryKeyByTable[TableName] = PrimaryKeyName;
        IsPrimaryKey = false;
    }
}

void TPgDumpParser::EndToken(bool isPrepare) {
    if (IsCommented) {
        LastToken.clear();
        return;
    }
    if (isPrepare) {
        ApplyToken(&PrepareRoot);
        PrimaryKeyCheck();
    } else {
        ApplyToken(&RunRoot);
        PgCatalogCheck();
        CreateTableCheck();
        AlterTableCheck();
    }
    LastToken.clear();
}

TString TPgDumpParser::ExtractToken(TString* result, const std::function<bool(char)>& pred) {
    auto pos = Buffer.size();
    while (pos > 0 && pred(Buffer[pos - 1])) {
        --pos;
        if (result) {
            *result += Buffer[pos];
        }
    }
    TString token = Buffer.substr(pos, Buffer.size() - pos);
    Buffer.remove(pos);
    return token;
}

void TPgDumpParser::PgCatalogCheck() {
    if (IsSelect) {
        IsSelect = false;
        if (TableName.StartsWith("pg_catalog.set_config")) {
            if (!IgnoreUnsupported) {
                throw yexception() << "\"SELECT pg_catalog.set_config.*\" statement is not supported.\n" <<
                    "Use \"--ignore-unsupported\" option if you want to ignore this statement.";
            }
            TString tmpBuffer;
            while (!Buffer.empty()) {
                auto token = ExtractToken(&tmpBuffer, [](char c){return !std::isspace(c);});
                if (token == "SELECT") {
                    break;
                }
                ExtractToken(&tmpBuffer, [](char c){return std::isspace(c);});
            }
            std::reverse(tmpBuffer.begin(), tmpBuffer.vend());
            Buffer += TStringBuilder() << "-- " << tmpBuffer;
            Cerr << TStringBuilder() << "-- " << tmpBuffer << Endl;
        }
    }
}

bool TPgDumpParser::IsPrimaryKeyTokens(const TVector<TString>& tokens) {
    const TVector<TString> desired = {"ALTER", "TABLE", "ONLY", "", "ADD", "CONSTRAINT", "", "PRIMARY", "KEY", "(", "", ")", ";"};
    if (tokens.size() != desired.size()) {
        return false;
    }
    for (size_t i = 0; i < desired.size(); ++i) {
        if (!desired[i].empty() && desired[i] != tokens[i]) {
            return false;
        }
    }
    return true;
}

void TPgDumpParser::AlterTableCheck() {
    if (IsAlterTable) {
        IsAlterTable = false;
        TString tmpBuffer;
        TVector <TString> tokens;
        while (!Buffer.empty()) {
            auto token = ExtractToken(&tmpBuffer, [](char c){return !std::isspace(c);});
            tokens.push_back(token);
            if (token == "ALTER") {
                break;
            }
            ExtractToken(&tmpBuffer, [](char c){return std::isspace(c) && c != '\n';});
            if (!Buffer.empty() && Buffer.back() == '\n') {
                tmpBuffer += " --";
            }
            ExtractToken(&tmpBuffer, [](char c){return std::isspace(c);});
        }
        std::reverse(tokens.begin(), tokens.end());
        if (!IgnoreUnsupported && !IsPrimaryKeyTokens(tokens)) {
            throw yexception() << "\"ALTER TABLE\" statement is not supported.\n" <<
                "Use \"--ignore-unsupported\" option if you want to ignore this statement.";
        }
        std::reverse(tmpBuffer.begin(), tmpBuffer.vend());
        Buffer += TStringBuilder() << "-- " << tmpBuffer;
        Cerr << TStringBuilder() << "-- " << tmpBuffer << Endl;
    }
}

void TPgDumpParser::CreateTableCheck() {
    if (!IsCreateTable && BracesCount > 0) {
        IsCreateTable = true;
    }
    if (IsCreateTable && BracesCount == 0) {
        IsCreateTable = false;
        Buffer.pop_back();
        while (!Buffer.empty() && std::isspace(Buffer.back())) {
            Buffer.pop_back();
        }
        if (PrimaryKeyByTable.find(TableName) != PrimaryKeyByTable.end()) {
            Buffer += TStringBuilder() << ",\n    PRIMARY KEY(" + PrimaryKeyByTable[TableName] + ")\n)";
        } else {
            Buffer += TStringBuilder() << ",\n    __ydb_stub_id BIGSERIAL PRIMARY KEY\n)";
        }
        PrimaryKeyByTable.erase(TableName);
    }
    if (IsWithStatement) {
        IsWithStatement = false;
        while (!Buffer.empty()) {
            auto token = ExtractToken(nullptr, [](char c){return !std::isspace(c);});
            ExtractToken(nullptr, [](char c){return std::isspace(c);});
            if (token == "WITH") {
                break;
            }
        }
    }
}

} // NYdb::NConsoleClient
