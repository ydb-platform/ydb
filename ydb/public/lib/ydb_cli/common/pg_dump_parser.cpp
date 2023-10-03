#include "pg_dump_parser.h"

namespace NYdb::NConsoleClient {

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

TPgDumpParser::TPgDumpParser() {
    auto saveTableName = [this] {
        FixPublicScheme();
        TableName = LastToken;
    };
    auto searchPrimaryKeyNode = Root->AddCommand("CREATE")->
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

    WithParsingRoot->AddCommand("WITH")->AddCommand("(")->AddOptionalCommand("")->
        AddCommand(")", [this]{IsWithStatement = true;});

    Root->AddCommand("INSERT")->AddCommand("INTO")->AddCommand("", saveTableName);
    Root->AddCommand("SELECT")->AddCommand("", saveTableName)->
        AddOptionalCommand("")->AddCommand(";", [this]{IsSelect = true;});
    Root->AddCommand("ALTER")->AddCommand("TABLE", [this]{IsAlterTable = true;})->AddCommand("ONLY")->
        AddCommand("", saveTableName)->AddCommand("ADD")->AddCommand("CONSTRAINT")->
        AddCommand("")->AddCommand("PRIMARY")->AddCommand("KEY")->
        AddCommand("(")->AddCommand("", [this]{PrimaryKeyName=LastToken;})->
        AddCommand(")")->AddCommand(";", [this]{IsPrimaryKey=true;});
}

void TPgDumpParser::AddChar(char c) {
    if (!LastToken.empty() && (std::isspace(c) || IsNewTokenSymbol(c) || IsNewTokenSymbol(Buffers.back().back()))) {
        EndToken();
    }
    if (!std::isspace(c)) {
        LastToken += c;
    }
    Buffers.back() += c;
}

void TPgDumpParser::WritePgDump(IOutputStream& stream) {
    EndToken();
    for (const auto& [name, id] : BufferIdByTableName) {
        TString& createTableBuffer = Buffers[id];
        createTableBuffer.pop_back();
        while (!createTableBuffer.empty() && std::isspace(createTableBuffer.back())) {
            createTableBuffer.pop_back();
        }
        createTableBuffer += TStringBuilder() << ",\n    __ydb_stub_id BIGSERIAL PRIMARY KEY\n)";
    }
    for (const auto& buffer : Buffers) {
        stream << buffer;
    }
    Buffers.clear();
}

void TPgDumpParser::FixPublicScheme() {
    if (LastToken.StartsWith("public.")) {
        Buffers.back().remove(Buffers.back().size() - LastToken.size());
        TStringBuf token = LastToken;
        token.remove_prefix(TString("public.").size());
        Buffers.back() += token;
    }
}

void TPgDumpParser::ApplyToken() {
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
    if (CurrentNode != Root.get()) {
        CurrentNode = Root.get();
        if (IsAlterTable) {
            IsCommentAlterTable = true;
            IsAlterTable = false;
        }
        ApplyToken();
    }
}

void TPgDumpParser::EndToken() {
    ApplyToken();
    PgCatalogCheck();
    CreateTableCheck();
    AlterTableCheck();
    LastToken.clear();
}

TString TPgDumpParser::ExtractToken(TString* result, const std::function<bool(char)>& pred) {
    auto pos = Buffers.back().size();
    while (pos > 0 && pred(Buffers.back()[pos - 1])) {
        --pos;
        if (result) {
            *result += Buffers.back()[pos];
        }
    }
    TString token = Buffers.back().substr(pos, Buffers.back().size() - pos);
    Buffers.back().remove(pos);
    return token;
}

void TPgDumpParser::PgCatalogCheck() {
    if (IsSelect) {
        IsSelect = false;
        if (TableName.StartsWith("pg_catalog.set_config")) {
            TString tmpBuffer;
            while (!Buffers.back().empty()) {
                auto token = ExtractToken(&tmpBuffer, [](char c){return !std::isspace(c);});
                if (token == "SELECT") {
                    break;
                }
                ExtractToken(&tmpBuffer, [](char c){return std::isspace(c);});
            }
            std::reverse(tmpBuffer.begin(), tmpBuffer.vend());
            Buffers.back() += TStringBuilder() << "-- " << tmpBuffer;
        }
    }
}

void TPgDumpParser::AlterTableCheck() {
    if (IsCommentAlterTable) {
        IsCommentAlterTable = false;
        TString tmpBuffer;
        while (!Buffers.back().empty()) {
            auto token = ExtractToken(&tmpBuffer, [](char c){return !std::isspace(c);});
            if (token == "ALTER") {
                break;
            }
            ExtractToken(&tmpBuffer, [](char c){return std::isspace(c);});
        }
        std::reverse(tmpBuffer.begin(), tmpBuffer.vend());
        Buffers.back() += TStringBuilder() << "-- " << tmpBuffer;
    }

    if (IsPrimaryKey) {
        IsPrimaryKey = false;
        IsAlterTable = false;
        if (BufferIdByTableName.find(TableName) != BufferIdByTableName.end()) {
            TString& createTableBuffer = Buffers[BufferIdByTableName[TableName]];
            createTableBuffer.pop_back();
            while (!createTableBuffer.empty() && std::isspace(createTableBuffer.back())) {
                createTableBuffer.pop_back();
            }
            createTableBuffer += TStringBuilder() << ",\n    PRIMARY KEY(" + PrimaryKeyName + ")\n)";
            BufferIdByTableName.erase(TableName);
        }
        while (!Buffers.back().empty()) {
            auto token = ExtractToken(nullptr, [](char c){return !std::isspace(c);});
            ExtractToken(nullptr, [](char c){return std::isspace(c);});
            if (token == "ALTER") {
                break;
            }
        }
    }
}

void TPgDumpParser::CreateTableCheck() {
    if (!IsCreateTable && BracesCount > 0) {
        IsCreateTable = true;
    }
    if (IsCreateTable && BracesCount == 0) {
        IsCreateTable = false;
        BufferIdByTableName[TableName] = Buffers.size() - 1;
        Buffers.push_back("");
        CurrentNode =
        WithParsingRoot.get();
    }
    if (IsWithStatement) {
        IsWithStatement = false;
        while (!Buffers.back().empty()) {
            auto token = ExtractToken(nullptr, [](char c){return !std::isspace(c);});
            ExtractToken(nullptr, [](char c){return std::isspace(c);});
            if (token == "WITH") {
                break;
            }
        }
    }
}

} // NYdb::NConsoleClient
