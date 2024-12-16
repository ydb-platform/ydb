#include "string_filter.h"

#include "consumer.h"
#include "pull_parser.h"
#include "token_writer.h"

#include <yt/yt/core/ypath/stack.h>
#include <yt/yt/core/ytree/convert.h>

#include <util/stream/mem.h>

namespace NYT::NYson {

using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

//! A class that is able to produce YSON strings from a "sparse" sequence of
//! YSON fragments equipped with YPath stacks defining their location in the YSON.
class TSparseYsonBuilder
{
public:
    //! Switch to the given stack, properly closing all not needed collections from
    //! the previous stack and opening new needed collections for the given stack.
    void SwitchStack(TYPathStack newStack)
    {
        if (IsFirstStack_) {
            OnFirstStack(newStack);
        } else {
            OnIntermediateStack(newStack);
        }

        Stack_ = std::move(newStack);
    }

    //! Returns either resulting YSON or the null YSON in case of no #SwitchStack calls.
    TYsonString Flush()
    {
        if (IsFirstStack_) {
            // This means that there were no matches, therefore we must return null YSON.
            return {};
        }

        // Close all currently open collections.
        OnAfterLastStack();

        // Then, flush the writer and return the resulting YSON.
        Writer_.Flush();
        Output_.Flush();
        YT_VERIFY(!Result_.empty());
        return TYsonString(std::move(Result_));
    }

    //! Access the underlying YSON writer for injecting YSON fragments.
    TCheckedInDebugYsonTokenWriter* GetWriter()
    {
        return &Writer_;
    }

private:
    TString Result_;
    TStringOutput Output_ = TStringOutput(Result_);
    TCheckedInDebugYsonTokenWriter Writer_ = TCheckedInDebugYsonTokenWriter(&Output_);
    TYPathStack Stack_;
    bool IsFirstStack_ = true;

    //! Open a map or a list for the given stack entry.
    void OpenEntry(const TYPathStack::TEntry& entry)
    {
        if (std::holds_alternative<TString>(entry)) {
            Writer_.WriteBeginMap();
            Writer_.WriteBinaryString(std::get<TString>(entry));
            Writer_.WriteKeyValueSeparator();
        } else {
            Writer_.WriteBeginList();
            // Do not forget to write entities until we are at a correct index.
            for (int index = 0; index < std::get<int>(entry); ++index) {
                Writer_.WriteEntity();
                Writer_.WriteItemSeparator();
            }
        }
    }

    //! Close a map or a list for the given stack entry.
    void CloseEntry(const TYPathStack::TEntry& entry)
    {
        if (std::holds_alternative<TString>(entry)) {
            Writer_.WriteEndMap();
        } else {
            Writer_.WriteEndList();
        }
    }

    //! Handle the special case of a first stack.
    void OnFirstStack(TYPathStack newStack)
    {
        IsFirstStack_ = false;
        // Open necessary collections for the first stack. Mind that the first entry is an artificial /0.
        const auto& newStackItems = newStack.Items();
        for (ssize_t index = 1; index < std::ssize(newStackItems); ++index) {
            OpenEntry(newStackItems[index]);
        }
    }

    //! Handle the case of a stack that is not the first one.
    void OnIntermediateStack(TYPathStack newStack)
    {
        const auto& oldStackItems = Stack_.Items();
        const auto& newStackItems = newStack.Items();

        ssize_t firstDifferingItemIndex = 0;
        while (
            firstDifferingItemIndex < std::ssize(oldStackItems) &&
            firstDifferingItemIndex < std::ssize(newStackItems) &&
            oldStackItems[firstDifferingItemIndex] == newStackItems[firstDifferingItemIndex])
        {
            ++firstDifferingItemIndex;
        }
        // Note that none of the two intermediate stacks may be a prefix of another.
        YT_VERIFY(firstDifferingItemIndex < std::ssize(oldStackItems));
        YT_VERIFY(firstDifferingItemIndex < std::ssize(newStackItems));

        // Close all collections that are not present in the new stack.
        for (auto index = std::ssize(oldStackItems) - 1; index > firstDifferingItemIndex; --index) {
            CloseEntry(oldStackItems[index]);
        }

        // Then, switch to a proper key or list index in the (common) collection of a first different entry.
        const auto& oldStackFirstDifferingItem = oldStackItems[firstDifferingItemIndex];
        const auto& newStackFirstDifferingItem = newStackItems[firstDifferingItemIndex];
        YT_VERIFY(
            std::holds_alternative<TString>(oldStackFirstDifferingItem) ==
            std::holds_alternative<TString>(newStackFirstDifferingItem));

        if (std::holds_alternative<TString>(oldStackFirstDifferingItem)) {
            // Switch key to a proper key.
            Writer_.WriteItemSeparator();
            Writer_.WriteBinaryString(std::get<TString>(newStackFirstDifferingItem));
            Writer_.WriteKeyValueSeparator();
        } else {
            // If current index and desired index are not adjacent, we must
            // introduce a proper number of entities between them.
            auto currentIndex = std::get<int>(oldStackFirstDifferingItem);
            auto newIndex = std::get<int>(newStackFirstDifferingItem);
            YT_VERIFY(currentIndex < newIndex);
            for (auto index = currentIndex; index + 1 < newIndex; ++index) {
                Writer_.WriteItemSeparator();
                Writer_.WriteEntity();
            }
            Writer_.WriteItemSeparator();
        }

        // Open all collections that are present in the new stack.
        for (auto index = firstDifferingItemIndex + 1; index < std::ssize(newStackItems); ++index) {
            OpenEntry(newStackItems[index]);
        }
    }

    //! Handle the last stack closing.
    void OnAfterLastStack()
    {
        // Close all currently open collections. Mind that the first entry is an artificial /0.
        const auto& oldStackItems = Stack_.Items();
        for (auto index = std::ssize(oldStackItems) - 1; index > 0; --index) {
            CloseEntry(oldStackItems[index]);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

//! A class that uses a sparse YSON builder to construct a YSON consisting of
//! fragments matching the given YPaths.
class TYsonFilterer
{
public:
    TYsonFilterer(TSparseYsonBuilder* builder, TYsonStringBuf yson, const std::vector<TYPath>& paths)
        : Builder_(builder)
        , Paths_(paths.begin(), paths.end())
        , Input_(yson.AsStringBuf())
        , Parser_(&Input_, EYsonType::Node)
        , Cursor_(&Parser_)
    {
        // We pretend that our YSON is enclosed into an outer list, and we are already
        // in a context of parsing. In particular, we stay behind its zeroth element,
        // therefore our stack initial value is "/-1".
        StackLevelIsList_ = {true};
        Stack_.Push(-1);
    }

    //! Return either a filtered YSON or an null YSON string if there were no matches.
    TYsonString Filter()
    {
        while (!Cursor_->IsEndOfStream()) {
            if (ConsumeItemPrologue()) {
                ConsumeItemEpilogue();
            } else {
                FinishComposite();
            }
        }

        return Builder_->Flush();
    }

private:
    TSparseYsonBuilder* Builder_;
    THashSet<TYPath> Paths_;

    TMemoryInput Input_;
    TYsonPullParser Parser_;
    TYsonPullParserCursor Cursor_;

    TYPathStack Stack_;
    std::vector<bool> StackLevelIsList_;

    [[noreturn]] void ThrowUnexpectedItemType(TStringBuf stage)
    {
        THROW_ERROR_EXCEPTION(
            "Unexpected item type %Qlv for %v in %v",
            Cursor_.GetCurrent().GetType(),
            StackLevelIsList_.back() ? "list" : "map",
            stage);
    }

    //! If the current stack corresponds to a requested path, pass the current
    //! composite to a sparse YSON builder.
    bool TryExtractComposite()
    {
        // Discard the first item of the stack which is always "/0" corresponding
        // to the position in the artificial enclosing list.
        constexpr int StaticPrefixLength = 2;
        const auto& path = TStringBuf(Stack_.GetPath()).substr(StaticPrefixLength);
        if (Paths_.contains(path)) {
            Builder_->SwitchStack(Stack_);
            Cursor_.TransferComplexValue(Builder_->GetWriter());
            return true;
        }
        return false;
    }

    //! A helper function that starts consuming a composite item. It returns true if
    //! current composite is not finished, and advances to the beginning of the value
    //! to consume (i.e. puts the key on stack in case of map and increments the index
    //! in case of list). Otherwise, it returns false and skips the closing bracket.
    bool ConsumeItemPrologue()
    {
        if (StackLevelIsList_.back()) {
            switch (Cursor_->GetType()) {
                case EYsonItemType::EndList:
                    return false;
                default:
                    // Increase the topmost ypath stack item.
                    Stack_.IncreaseLastIndex();
                    return true;
            }
        } else {
            switch (Cursor_->GetType()) {
                case EYsonItemType::EndMap:
                    return false;
                case EYsonItemType::StringValue:
                    // Set the topmost item of a stack to a current key.
                    Stack_.Pop();
                    Stack_.Push(Cursor_->UncheckedAsString());
                    Cursor_.Next();
                    return true;
                default:
                    ThrowUnexpectedItemType("prologue");
            }
        }
    }

    //! A helper function that finishes consuming a composite item. It either transfers
    //! a whole value to a sparse YSON builder in case of a match, descends into inner
    //! composite, or just skips the singular value.
    void ConsumeItemEpilogue()
    {
        if (TryExtractComposite()) {
            // We successfully extracted a composite, hooray!
            return;
        }

        if (Cursor_->GetType() == EYsonItemType::BeginAttributes) {
            // For now, do not descend into attributes, so just skip them entirely.
            Cursor_.SkipAttributes();
        }

        switch (Cursor_->GetType()) {
            case EYsonItemType::BeginMap:
                StackLevelIsList_.push_back(false);
                // Push a placeholder that will be replaced by the actual value of a first key on
                // the next iteration.
                Stack_.Push("");
                Cursor_.Next();
                break;
            case EYsonItemType::BeginList:
                StackLevelIsList_.push_back(true);
                // Push a placeholder that will be incremented on the next iteration, producing
                // zero as an index of the first item of the list.
                Stack_.Push(-1);
                Cursor_.Next();
                break;
            case EYsonItemType::StringValue:
            case EYsonItemType::BooleanValue:
            case EYsonItemType::DoubleValue:
            case EYsonItemType::Int64Value:
            case EYsonItemType::Uint64Value:
            case EYsonItemType::EntityValue:
                // Keeping in mind that maybeExtractComposite() returned false,
                // this singular value is of no interest to us, so just skip it.
                Cursor_.Next();
                break;
            case EYsonItemType::EndList:
            case EYsonItemType::EndMap:
            case EYsonItemType::BeginAttributes:
            case EYsonItemType::EndAttributes:
            case EYsonItemType::EndOfStream:
                // This may happen only in case of incorrect YSON, e.g.
                // "{42;]", "{foo=}", "<foo=bar><boo=far>42", "[>]" or "[foo".
                ThrowUnexpectedItemType("epilogue");
        }
    }

    //! A helper to move out of a finished composite.
    void FinishComposite()
    {
        StackLevelIsList_.pop_back();
        Stack_.Pop();
        Cursor_.Next();
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Returns either an empty map, empty list or an original scalar value depending
//! on YSON value type. Topmost attributes are discarded.
TYsonString FilterYsonStringFallback(TYsonStringBuf yson)
{
    TMemoryInput input(yson.AsStringBuf());
    TYsonPullParser parser(&input, EYsonType::Node);
    TYsonPullParserCursor cursor(&parser);

    // We always drop attributes in this case.
    if (cursor->GetType() == EYsonItemType::BeginAttributes) {
        cursor.SkipAttributes();
    }
    switch (cursor->GetType()) {
        case EYsonItemType::BeginList:
            return TYsonString(TString("[]"));
        case EYsonItemType::BeginMap:
            return TYsonString(TString("{}"));
        case EYsonItemType::StringValue:
        case EYsonItemType::Int64Value:
        case EYsonItemType::Uint64Value:
        case EYsonItemType::DoubleValue:
        case EYsonItemType::BooleanValue:
        case EYsonItemType::EntityValue: {
            // Copy the value to a new YSON string and return it.
            TString result;
            TStringOutput output(result);
            TCheckedInDebugYsonTokenWriter writer(&output);
            cursor.TransferComplexValue(&writer);
            writer.Flush();
            output.Flush();
            return TYsonString(std::move(result));
        }
        default:
            // This is possible only in case of malformed YSON.
            THROW_ERROR_EXCEPTION("Unexpected YSON item type %Qlv after attributes", cursor->GetType());
    }
}

////////////////////////////////////////////////////////////////////////////////

TYsonString FilterYsonString(
    const std::vector<TYPath>& paths,
    TYsonStringBuf yson,
    bool allowNullResult)
{
    TSparseYsonBuilder builder;
    TYsonFilterer filterer(&builder, yson, paths);
    if (auto result = filterer.Filter(); result || allowNullResult) {
        return result;
    } else {
        return FilterYsonStringFallback(yson);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
