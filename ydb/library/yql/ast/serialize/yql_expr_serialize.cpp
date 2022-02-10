#include "yql_expr_serialize.h"
#include <ydb/library/yql/minikql/pack_num.h>
#include <util/generic/algorithm.h>
#include <util/generic/deque.h>

namespace NYql {

namespace {

enum ESerializeCommands {
    NODE_REF = 0x00,
    NODE_VALUE = 0x10,
    INLINE_STR = 0x08, // string is unique, don't write it to the pool
    SAME_POSITION = 0x40,
    ATOM_FLAG = 0x20,
    WIDE = 0x80, // mark wide lambdas
    ATOM = ATOM_FLAG | NODE_VALUE,  // for atoms we will use TNodeFlags bits (1/2/4)
    LIST = TExprNode::List | NODE_VALUE,
    CALLABLE = TExprNode::Callable | NODE_VALUE,
    LAMBDA = TExprNode::Lambda | NODE_VALUE,
    ARGUMENT = TExprNode::Argument | NODE_VALUE,
    ARGUMENTS = TExprNode::Arguments | NODE_VALUE,
    WORLD = TExprNode::World | NODE_VALUE,
};

using namespace NKikimr;

class TWriter {
public:
    TWriter(TExprContext& ctx, ui16 components)
        : Ctx(ctx)
        , Components_(components)
    {
    }

    const TString& Out() const {
        //Cerr << "Nodes:" << WrittenNodes_.size() << ", pos: " << Positions_.size() << ", bytes: " << Out_.size() << "\n";
        return Out_;
    }

    void Prepare(const TExprNode& node) {
        TNodeSet visited;
        PrepareImpl(node, visited);
    }

    void Init() {
        WriteVar32(Components_);
        ui32 reusedStringCount = 0;
        for (auto& x : StringCounters_) {
            if (x.second.first > 1) {
                x.second.second = reusedStringCount;
                ++reusedStringCount;
            }
        }

        WriteVar32(reusedStringCount);
        TVector<std::pair<TStringBuf, ui32>> sortedStrings;
        sortedStrings.reserve(reusedStringCount);
        for (const auto& x : StringCounters_) {
            if (x.second.first > 1) {
                sortedStrings.push_back({ x.first, x.second.second });
            }
        }

        Sort(sortedStrings.begin(), sortedStrings.end(), [](const auto& x, const auto& y) { return x.second < y.second; });

        for (const auto& x : sortedStrings) {
            WriteVar32(x.first.length());
            WriteMany(x.first.data(), x.first.length());
        }

        if (Components_ & TSerializedExprGraphComponents::Positions) {
            WriteVar32(Files_.size());
            TVector<std::pair<TStringBuf, ui32>> sortedFiles;
            sortedFiles.reserve(Files_.size());
            for (const auto& x : Files_) {
                sortedFiles.push_back({ x.first, x.second });
            }

            Sort(sortedFiles.begin(), sortedFiles.end(), [](const auto& x, const auto& y) { return x.second < y.second; });
            for (const auto& x : sortedFiles) {
                WriteVar32(x.first.length());
                WriteMany(x.first.data(), x.first.length());
            }

            WriteVar32(Positions_.size());
            TVector<std::tuple<ui32, ui32, ui32, ui32>> sortedPositions;
            sortedPositions.reserve(Positions_.size());
            for (const auto& x : Positions_) {
                sortedPositions.push_back({ std::get<0>(x.first), std::get<1>(x.first), std::get<2>(x.first), x.second });
            }

            Sort(sortedPositions.begin(), sortedPositions.end(), [](const auto& x, const auto& y)
                { return std::get<3>(x) < std::get<3>(y); });

            for (const auto& x : sortedPositions) {
                WriteVar32(std::get<0>(x));
                WriteVar32(std::get<1>(x));
                WriteVar32(std::get<2>(x));
            }
        }
    }

    void Save(const TExprNode& node) {
        auto writtenIt = WrittenNodes_.find(&node);
        if (writtenIt != WrittenNodes_.end()) {
            Write(NODE_REF);
            WriteVar32(writtenIt->second);
            return;
        }

        char command = (node.Type() == TExprNode::Atom) ? ATOM : ((node.Type() & TExprNode::TypeMask) | NODE_VALUE);

        if (node.Type() == TExprNode::Lambda && node.ChildrenSize() > 2U) {
            command |= WIDE;
        }

        if (Components_ & TSerializedExprGraphComponents::Positions) {
            // will write position
            if (Ctx.GetPosition(node.Pos()) == LastPosition_) {
                command |= SAME_POSITION;
            }
        }

        if (node.Type() == TExprNode::Atom) {
            command |= (TNodeFlags::FlagsMask & node.Flags());
        }

        ui32 strNum = 0;
        if (node.Type() == TExprNode::Atom || node.Type() == TExprNode::Callable || node.Type() == TExprNode::Argument) {
            auto strIt = StringCounters_.find(node.Content());
            YQL_ENSURE(strIt != StringCounters_.end());
            if (strIt->second.first == 1) {
                command |= INLINE_STR;
            } else {
                strNum = strIt->second.second;
            }
        }

        Write(command);
        if ((Components_ & TSerializedExprGraphComponents::Positions) && !(command & SAME_POSITION)) {
            const auto& pos = Ctx.GetPosition(node.Pos());
            ui32 fileNum = 0;
            if (pos.File) {
                auto fileIt = Files_.find(pos.File);
                YQL_ENSURE(fileIt != Files_.end());
                fileNum = fileIt->second;
            }

            auto posIt = Positions_.find(std::make_tuple(std::move(pos.Row), std::move(pos.Column),
                std::move(fileNum)));
            YQL_ENSURE(posIt != Positions_.end());
            WriteVar32(posIt->second);
            LastPosition_ = pos;
        }

        if (node.Type() == TExprNode::Atom || node.Type() == TExprNode::Callable || node.Type() == TExprNode::Argument) {
            if (command & INLINE_STR) {
                WriteVar32(node.Content().length());
                WriteMany(node.Content().data(), node.Content().length());
            } else {
                WriteVar32(strNum);
            }
        }

        if (node.Type() == TExprNode::Callable || node.Type() == TExprNode::Arguments || node.Type() == TExprNode::List || (node.Type() == TExprNode::Lambda && node.ChildrenSize() > 2U)) {
            WriteVar32(node.ChildrenSize());
        }

        for (const auto& x : node.Children()) {
            Save(*x);
        }

        WrittenNodes_.emplace(&node, 1 + WrittenNodes_.size());
    }

private:
    void PrepareImpl(const TExprNode& node, TNodeSet& visited) {
        if (!visited.emplace(&node).second) {
            return;
        }

        if (Components_ & TSerializedExprGraphComponents::Positions) {
            const auto& pos = Ctx.GetPosition(node.Pos());
            const auto& file = pos.File;
            ui32 fileNum = 0;
            if (file) {
                fileNum = Files_.emplace(file, 1 + (ui32)Files_.size()).first->second;
            }

            Positions_.emplace(std::make_tuple(std::move(pos.Row), std::move(pos.Column),
                std::move(fileNum)), (ui32)Positions_.size());
        }

        if (node.IsAtom() || node.IsCallable() || node.Type() == TExprNode::Argument) {
            auto& x = StringCounters_[node.Content()];
            x.first++;
        }

        for (const auto& x : node.Children()) {
            PrepareImpl(*x, visited);
        }
    }

    Y_FORCE_INLINE void Write(char c) {
        Out_.append(c);
    }

    Y_FORCE_INLINE void WriteMany(const void* buf, size_t len) {
        Out_.AppendNoAlias((const char*)buf, len);
    }

    Y_FORCE_INLINE void WriteVar32(ui32 value) {
        char buf[MAX_PACKED32_SIZE];
        Out_.AppendNoAlias(buf, Pack32(value, buf));
    }

private:
    TExprContext& Ctx;
    const ui16 Components_;
    THashMap<TStringBuf, ui32> Files_;
    THashMap<std::tuple<ui32, ui32, ui32>, ui32> Positions_;
    THashMap<TStringBuf, std::pair<ui32, ui32>> StringCounters_; // str -> id + serialized id

    TNodeMap<ui32> WrittenNodes_;
    TPosition LastPosition_;

    TString Out_;
};

class TReader {
public:
    TReader(TPosition pos, TStringBuf buffer, TExprContext& ctx)
        : Pos_(pos)
        , Current_(buffer.Data())
        , End_(buffer.Data() + buffer.Size())
        , Ctx_(ctx)
        , Components_(0)
    {
    }

    TExprNode::TPtr Load() {
        try {
            Components_ = ReadVar32();
            auto reusedStringCount = ReadVar32();
            Strings_.reserve(reusedStringCount);
            for (ui32 i = 0; i < reusedStringCount; ++i) {
                ui32 length = ReadVar32();
                auto internedBuf = Ctx_.AppendString(TStringBuf(ReadMany(length), length));
                Strings_.push_back(internedBuf);
            }

            if (Components_ & TSerializedExprGraphComponents::Positions) {
                auto filesCount = ReadVar32();
                Files_.reserve(filesCount);
                for (ui32 i = 0; i < filesCount; ++i) {
                    ui32 length = ReadVar32();
                    TStringBuf file(ReadMany(length), length);
                    Files_.push_back(TString(file));
                }

                auto positionsCount = ReadVar32();
                Positions_.reserve(positionsCount);
                for (ui32 i = 0; i < positionsCount; ++i) {
                    ui32 row = ReadVar32();
                    ui32 column = ReadVar32();
                    ui32 fileNum = ReadVar32();
                    if (fileNum > Files_.size()) {
                        ThrowCorrupted();
                    }

                    Positions_.push_back({ row, column, fileNum });
                }
            }

            TExprNode::TPtr result = Fetch();
            if (Current_ != End_) {
                ThrowCorrupted();
            }

            return result;
        } catch (const yexception& e) {
            TIssue issue(Pos_, TStringBuilder() << "Failed to deserialize expression graph, reason:\n" << e.what());
            issue.SetCode(UNEXPECTED_ERROR, ESeverity::TSeverityIds_ESeverityId_S_FATAL);
            Ctx_.AddError(issue);
            return nullptr;
        }
    }

private:
    TExprNode::TPtr Fetch() {
        char command = Read();
        if (!(command & NODE_VALUE)) {
            ui32 nodeId = ReadVar32();
            if (nodeId == 0 || nodeId > Nodes_.size()) {
                ThrowCorrupted();
            }

            return Nodes_[nodeId - 1];
        }


        command &= ~NODE_VALUE;
        TPosition pos = Pos_;
        if (Components_ & TSerializedExprGraphComponents::Positions) {
            if (command & SAME_POSITION) {
                pos = LastPosition_;
                command &= ~SAME_POSITION;
            } else {
                ui32 posNum = ReadVar32();
                if (posNum >= Positions_.size()) {
                    ThrowCorrupted();
                }

                const auto& posItem = Positions_[posNum];

                pos = TPosition();
                pos.Row = std::get<0>(posItem);
                pos.Column = std::get<1>(posItem);
                auto fileNum = std::get<2>(posItem);
                if (fileNum > 0) {
                    pos.File = Files_[fileNum - 1];
                }

                LastPosition_ = pos;
            }
        }

        ui32 atomFlags = 0;
        bool hasInlineStr = command & INLINE_STR;
        command &= ~INLINE_STR;
        if (command & ATOM_FLAG) {
            atomFlags = command & TNodeFlags::FlagsMask;
            command &= ~(ATOM_FLAG | TNodeFlags::FlagsMask);
            command |= TExprNode::Atom;
        }

        const bool wide = command & WIDE;
        command &= ~WIDE;

        TStringBuf content;
        if (command == TExprNode::Atom || command == TExprNode::Callable || command == TExprNode::Argument) {
            if (hasInlineStr) {
                ui32 length = ReadVar32();
                content = TStringBuf(ReadMany(length), length);
            } else {
                ui32 strNum = ReadVar32();
                if (strNum >= Strings_.size()) {
                    ThrowCorrupted();
                }

                content = Strings_[strNum];
            }
        }

        ui32 childrenSize = 0;
        if (command == TExprNode::Callable || command == TExprNode::Arguments || command == TExprNode::List || (command == TExprNode::Lambda && wide)) {
            childrenSize = ReadVar32();
        }

        TExprNode::TPtr ret;
        switch (command) {
        case TExprNode::Atom:
            ret = Ctx_.NewAtom(pos, content, atomFlags);
            break;
        case TExprNode::List: {
            TExprNode::TListType children;
            children.reserve(childrenSize);
            for (ui32 i = 0U; i < childrenSize; ++i) {
                children.emplace_back(Fetch());
            }

            ret = Ctx_.NewList(pos, std::move(children));
            break;
        }

        case TExprNode::Callable: {
            TExprNode::TListType children;
            children.reserve(childrenSize);
            for (ui32 i = 0U; i < childrenSize; ++i) {
                children.emplace_back(Fetch());
            }

            ret = Ctx_.NewCallable(pos, content, std::move(children));
            break;
        }

        case TExprNode::Argument:
            ret = Ctx_.NewArgument(pos, content);
            break;

        case TExprNode::Arguments: {
            TExprNode::TListType children;
            children.reserve(childrenSize);
            for (ui32 i = 0U; i < childrenSize; ++i) {
                children.emplace_back(Fetch());
            }

            ret = Ctx_.NewArguments(pos, std::move(children));
            break;
        }

        case TExprNode::Lambda:
            if (wide) {
                TExprNode::TListType children;
                children.reserve(childrenSize);
                for (ui32 i = 0U; i < childrenSize; ++i) {
                    children.emplace_back(Fetch());
                }
                ret = Ctx_.NewLambda(pos, std::move(children));
            } else {
                auto args = Fetch();
                auto body = Fetch();
                ret = Ctx_.NewLambda(pos, {std::move(args), std::move(body)});
            }
            break;

        case TExprNode::World:
            ret = Ctx_.NewWorld(pos);
            break;

        default:
            ThrowCorrupted();
        }

        Nodes_.push_back(ret);
        return ret;
    }

    Y_FORCE_INLINE char Read() {
        if (Current_ == End_)
            ThrowNoData();

        return *Current_++;
    }

    Y_FORCE_INLINE const char* ReadMany(ui32 count) {
        if (Current_ + count > End_)
            ThrowNoData();

        const char* result = Current_;
        Current_ += count;
        return result;
    }

    Y_FORCE_INLINE ui32 ReadVar32() {
        ui32 result = 0;
        size_t count = Unpack32(Current_, End_ - Current_, result);
        if (!count) {
            ThrowCorrupted();
        }
        Current_ += count;
        return result;
    }

    [[noreturn]] static void ThrowNoData() {
        ythrow yexception() << "No more data in buffer";
    }

    [[noreturn]] static void ThrowCorrupted() {
        ythrow yexception() << "Serialized data is corrupted";
    }

private:
    const TPosition Pos_;
    const char* Current_;
    const char* const End_;
    TExprContext& Ctx_;
    ui16 Components_;

    TVector<TStringBuf> Strings_;
    TVector<TString> Files_;
    TVector<std::tuple<ui32, ui32, ui32>> Positions_;

    TPosition LastPosition_;
    TDeque<TExprNode::TPtr> Nodes_;
};

}

TString SerializeGraph(const TExprNode& node, TExprContext& ctx, ui16 components) {
    TWriter writer(ctx, components);
    writer.Prepare(node);
    writer.Init();
    writer.Save(node);
    return writer.Out();
}

TExprNode::TPtr DeserializeGraph(TPositionHandle pos, TStringBuf buffer, TExprContext& ctx) {
    return DeserializeGraph(ctx.GetPosition(pos), buffer, ctx);
}

TExprNode::TPtr DeserializeGraph(TPosition pos, TStringBuf buffer, TExprContext& ctx) {
    TReader reader(pos, buffer, ctx);
    return reader.Load();
}

} // namespace NYql

