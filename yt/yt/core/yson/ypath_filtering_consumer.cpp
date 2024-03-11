#include "ypath_filtering_consumer.h"

#include "null_consumer.h"

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ypath/tokenizer.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TAtTokenTag
{ };

////////////////////////////////////////////////////////////////////////////////

class TYPathFilteringConsumerBase
    : public NYson::TYsonConsumerBase
{
public:
    void OnStringScalar(TStringBuf value) override
    {
        BeforeValue();
        Forward_->OnStringScalar(value);
    }

    void OnInt64Scalar(i64 value) override
    {
        BeforeValue();
        Forward_->OnInt64Scalar(value);
    }

    void OnUint64Scalar(ui64 value) override
    {
        BeforeValue();
        Forward_->OnUint64Scalar(value);
    }

    void OnDoubleScalar(double value) override
    {
        BeforeValue();
        Forward_->OnDoubleScalar(value);
    }

    void OnBooleanScalar(bool value) override
    {
        BeforeValue();
        Forward_->OnBooleanScalar(value);
    }

    void OnEntity() override
    {
        BeforeValue();
        Forward_->OnEntity();
    }

    void OnBeginMap() override
    {
        BeforeValue();
        AfterCollectionBegin_ = true;

        Forward_->OnBeginMap();
    }

    void OnKeyedItem(TStringBuf key) override
    {
        if (!AfterCollectionBegin_) {
            RollbackPath();
        }
        AfterCollectionBegin_ = false;
        AppendPath(key);

        Forward_->OnKeyedItem(key);
    }

    void OnEndMap() override
    {
        if (!AfterCollectionBegin_) {
            RollbackPath();
        }
        AfterCollectionBegin_ = false;
        BeforeEndDictionary();
        Forward_->OnEndMap();
    }

    void OnBeginList() override
    {
        BeforeValue();
        AfterCollectionBegin_ = true;
        ListIndexes_.push_back(0);

        Forward_->OnBeginList();
    }

    void OnListItem() override
    {
        if (!AfterCollectionBegin_) {
            RollbackPath();
        }
        AfterCollectionBegin_ = false;
        AppendPath(ListIndexes_.back());
        ++ListIndexes_.back();

        Forward_->OnListItem();
    }

    void OnEndList() override
    {
        if (!AfterCollectionBegin_) {
            RollbackPath();
        }
        AfterCollectionBegin_ = false;
        ListIndexes_.pop_back();

        Forward_->OnEndList();

    }

    void OnBeginAttributes() override
    {
        AppendPath(TAtTokenTag{});

        AfterCollectionBegin_ = true;

        Forward_->OnBeginAttributes();
    }

    void OnEndAttributes() override
    {
        if (!AfterCollectionBegin_) {
            RollbackPath();
        }

        BeforeEndDictionary();
        Forward_->OnEndAttributes();
        RollbackPath();
    }

protected:
    virtual void AppendPath(TStringBuf literal) = 0;
    virtual void AppendPath(int listIndex) = 0;
    virtual void AppendPath(TAtTokenTag) = 0;

    virtual void RollbackPath() = 0;

    virtual void BeforeValue() = 0;
    virtual void BeforeEndDictionary() = 0;

    void SetForward(NYson::IYsonConsumer* consumer)
    {
        Forward_ = consumer;
    }

    void ResetForward()
    {
        Forward_ = GetNullYsonConsumer();
    }

    NYson::IYsonConsumer* GetForward()
    {
        return Forward_;
    }

private:
    NYson::IYsonConsumer* Forward_;
    bool AfterCollectionBegin_ = false;
    std::vector<int> ListIndexes_;
};

////////////////////////////////////////////////////////////////////////////////

class TYPathFilteringConsumer
    : public TYPathFilteringConsumerBase
{
public:
    TYPathFilteringConsumer(
        NYson::IYsonConsumer* underlying,
        std::vector<NYPath::TYPath> paths,
        EPathFilteringMode mode)
        : Underlying_(underlying)
        , Paths_(std::move(paths))
        , Mode_(mode)
    {
        SetForward(Underlying_);
        PerPathFilteringStates_.reserve(Paths_.size());
        for (int i = 0; i < std::ssize(Paths_); ++i) {
            PerPathFilteringStates_.push_back(TPathFilteringState{
                .Tokenizer = NYPath::TTokenizer(Paths_[i]),
                .MaxMatchedDepth = 0,
                .Fulfilled = false,
                .Index = i
            });
        }

        for (auto& state : PerPathFilteringStates_) {
            state.Tokenizer.Expect(NYPath::ETokenType::StartOfStream);
            if (!ToNextLiteral(state.Tokenizer)) {
                state.Fulfilled = true;
                SubtreeFiltering_ = true;
                FilteringDepth_ = Depth_;
            }
        }
    }

private:
    struct TPathFilteringState
    {
        NYPath::TTokenizer Tokenizer;
        int MaxMatchedDepth;
        bool Fulfilled;
        int Index;
    };

    NYson::IYsonConsumer* Underlying_;
    const std::vector<NYPath::TYPath> Paths_;
    const EPathFilteringMode Mode_;
    bool SubtreeFiltering_ = false;
    int FilteringDepth_ = -1;
    std::vector<TPathFilteringState> PerPathFilteringStates_;
    int Depth_ = 0;

    // For asterisk (*) matching support only.
    std::vector<TPathFilteringState> SavedFilteringStates_;

    template <typename TTokenType>
    void AppendPathImpl(TTokenType token)
    {
        ++Depth_;

        bool advancedAnyPath = false;

        for (auto& state : PerPathFilteringStates_) {
            if (Depth_ != state.MaxMatchedDepth + 1 || state.Fulfilled) {
                continue;
            }

            if (DoesMatch(state.Tokenizer, token)) {
                if (state.Tokenizer.GetType() == NYPath::ETokenType::Asterisk) {
                    SaveFilteringState(state);
                }

                ++state.MaxMatchedDepth;
                advancedAnyPath = true;
                if (!ToNextLiteral(state.Tokenizer)) {
                    state.Fulfilled = true;

                    if (!SubtreeFiltering_) {
                        if (IsBlacklistMode()) {
                            ResetForward();
                            SubtreeFiltering_ = true;
                            FilteringDepth_ = Depth_;
                        } else if (IsWhitelistMode()) {
                            SubtreeFiltering_ = true;
                            FilteringDepth_ = Depth_;
                        }
                    }
                }
            }
        }

        if (IsWhitelistMode() && !SubtreeFiltering_) {
            if (!advancedAnyPath) {
                SubtreeFiltering_ = true;
                FilteringDepth_ = Depth_;
                ResetForward();
            }
        }
    }

    void AppendPath(TStringBuf literal) override
    {
        AppendPathImpl(literal);
    }

    void AppendPath(int listIndex) override
    {
        AppendPathImpl<TStringBuf>(NYPath::ToYPathLiteral(listIndex));
    }

    void AppendPath(TAtTokenTag tag) override
    {
        AppendPathImpl(tag);
    }

    void RollbackPath() override
    {
        if (SubtreeFiltering_ && Depth_ == FilteringDepth_) {
            SubtreeFiltering_ = false;
            SetForward(Underlying_);
        }

        --Depth_;

        if (!IsForcedEntitiesMode()) {
            while (!SavedFilteringStates_.empty() && SavedFilteringStates_.back().MaxMatchedDepth == Depth_) {
                PerPathFilteringStates_[SavedFilteringStates_.back().Index] = std::move(SavedFilteringStates_.back());
                SavedFilteringStates_.pop_back();
            }
        }
    }

    void BeforeEndDictionary() override
    {
        if (!IsForcedEntitiesMode()) {
            return;
        }

        for (auto& state : PerPathFilteringStates_) {
            if (Depth_ != state.MaxMatchedDepth || state.Fulfilled) {
                continue;
            }

            auto& tokenizer = state.Tokenizer;
            std::vector<bool> isAttributesStack;
            tokenizer.Expect(NYPath::ETokenType::Literal);
            GetForward()->OnKeyedItem(tokenizer.GetLiteralValue());
            while(ToNextLiteral(tokenizer)) {
                if (tokenizer.GetType() == NYPath::ETokenType::At) {
                    isAttributesStack.push_back(true);
                    GetForward()->OnBeginAttributes();
                    tokenizer.Advance();
                } else {
                    isAttributesStack.push_back(false);
                    GetForward()->OnBeginMap();
                }
                tokenizer.Expect(NYPath::ETokenType::Literal);
                GetForward()->OnKeyedItem(tokenizer.GetLiteralValue());
            }
            GetForward()->OnEntity();
            tokenizer.Expect(NYPath::ETokenType::EndOfStream);
            while(!isAttributesStack.empty()) {
                if (isAttributesStack.back()) {
                    GetForward()->OnEndAttributes();
                } else {
                    GetForward()->OnEndMap();
                }
                isAttributesStack.pop_back();
            }

            state.Fulfilled = true;
        }
    }

    void BeforeValue() override
    {
        for (auto& state : PerPathFilteringStates_) {
            if (Depth_ != state.MaxMatchedDepth || state.Fulfilled) {
                continue;
            }

            if (state.Tokenizer.GetType() == NYPath::ETokenType::At) {
                OnBeginAttributes();
                OnEndAttributes();
                return;
            }
        }
    }

    bool ToNextLiteral(NYPath::TTokenizer& tokenizer)
    {
        if (tokenizer.GetType() == NYPath::ETokenType::At) {
            return tokenizer.Advance() != NYPath::ETokenType::EndOfStream;
        }
        tokenizer.Advance();
        if (tokenizer.GetType() == NYPath::ETokenType::EndOfStream) {
            return false;
        } else {
            tokenizer.Expect(NYPath::ETokenType::Slash);
            tokenizer.Advance();
            return true;
        }
    }

    bool IsBlacklistMode() const
    {
        return Mode_ == EPathFilteringMode::Blacklist;
    }

    bool IsWhitelistMode() const
    {
        return Mode_ == EPathFilteringMode::Whitelist || Mode_ == EPathFilteringMode::WhitelistWithForcedEntities;
    }

    bool IsForcedEntitiesMode() const
    {
        return Mode_ == EPathFilteringMode::ForcedEntities || Mode_ == EPathFilteringMode::WhitelistWithForcedEntities;
    }

    template <typename TTokenType>
    bool DoesMatch(const NYPath::TTokenizer& tokenizer, TTokenType) const
    {
        if (tokenizer.GetType() == NYPath::ETokenType::At) {
            return std::is_same_v<TTokenType, TAtTokenTag>;
        }

        return false;
    }

    template <>
    bool DoesMatch<TStringBuf>(const NYPath::TTokenizer& tokenizer, TStringBuf token) const
    {
        if (tokenizer.GetType() == NYPath::ETokenType::Asterisk) {
            THROW_ERROR_EXCEPTION_IF(IsForcedEntitiesMode(),
                "YPathFilteringConsumer does not allow asterisk matching in forced entities modes");
            return true;
        }

        tokenizer.Expect(NYPath::ETokenType::Literal);
        return tokenizer.GetLiteralValue() == token;
    }

    void SaveFilteringState(const TPathFilteringState& state)
    {
        SavedFilteringStates_.push_back(TPathFilteringState{
            .Tokenizer = NYPath::TTokenizer(state.Tokenizer.GetInput()),
            .MaxMatchedDepth = state.MaxMatchedDepth,
            .Fulfilled = state.Fulfilled,
            .Index = state.Index,
        });

        SavedFilteringStates_.back().Tokenizer.Skip(NYPath::ETokenType::StartOfStream);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NYson::IYsonConsumer> CreateYPathFilteringConsumer(
    NYson::IYsonConsumer* underlying,
    std::vector<NYPath::TYPath> paths,
    EPathFilteringMode mode)
{
    return std::make_unique<TYPathFilteringConsumer>(underlying, std::move(paths), mode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
