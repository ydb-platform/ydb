#include "ypath_designated_consumer.h"
#include "forwarding_consumer.h"
#include "null_consumer.h"

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ypath/token.h>
#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

class TYPathDesignatedYsonConsumer
    : public TForwardingYsonConsumer
{
public:
    TYPathDesignatedYsonConsumer(
        NYPath::TYPath path,
        EMissingPathMode missingPathMode,
        IYsonConsumer* underlyingConsumer)
        : Path_(std::move(path))
        , MissingPathMode_(missingPathMode)
        , Tokenizer_(Path_)
        , UnderlyingConsumer_(underlyingConsumer)
    {
        Tokenizer_.Skip(NYPath::ETokenType::StartOfStream);
        ForwardIfPathIsExhausted();
    }

    void OnMyStringScalar(TStringBuf /*value*/) override
    {
        CheckAndThrowResolveError();
    }

    void OnMyInt64Scalar(i64 /*value*/) override
    {
        CheckAndThrowResolveError();
    }

    void OnMyUint64Scalar(ui64 /*value*/) override
    {
        CheckAndThrowResolveError();
    }

    void OnMyDoubleScalar(double /*value*/) override
    {
        CheckAndThrowResolveError();
    }

    void OnMyBooleanScalar(bool /*value*/) override
    {
        CheckAndThrowResolveError();
    }

    void OnMyEntity() override
    {
        CheckAndThrowResolveError();
    }

    void OnMyBeginList() override
    {
        if (State_ == EConsumerState::FollowingPath) {
            Tokenizer_.Skip(NYPath::ETokenType::Slash);
            CurrentListItemIndex_ = -1;
        }
    }

    void OnMyListItem() override
    {
        if (State_ == EConsumerState::ConsumptionCompleted) {
            return;
        }

        ++CurrentListItemIndex_;
        switch (Tokenizer_.GetType()) {
            case NYPath::ETokenType::Literal:
                int indexInPath;
                if (!TryFromString(Tokenizer_.GetToken(), indexInPath)) {
                    CheckAndThrowResolveError();
                }
                if (indexInPath == CurrentListItemIndex_) {
                    Tokenizer_.Advance();
                    ForwardIfPathIsExhausted();
                } else {
                    SkipSubTree();
                }
                break;

            default:
                Tokenizer_.ThrowUnexpected();
        }
    }

    void OnMyEndList() override
    {
        CheckAndThrowResolveError();
    }

    void OnMyBeginMap() override
    {
        if (State_ == EConsumerState::FollowingPath) {
            Tokenizer_.Skip(NYPath::ETokenType::Slash);
        }
    }

    void OnMyKeyedItem(TStringBuf key) override
    {
        if (State_ == EConsumerState::ConsumptionCompleted) {
            return;
        }

        switch (Tokenizer_.GetType()) {
            case NYPath::ETokenType::Literal:
                if (key == Tokenizer_.GetToken()) {
                    Tokenizer_.Advance();
                    ForwardIfPathIsExhausted();
                } else {
                    SkipSubTree();
                }
                break;
            case NYPath::ETokenType::At:
                if (MissingPathMode_ == EMissingPathMode::ThrowError) {
                    ThrowNoAttributes();
                }
                State_ = EConsumerState::ConsumptionCompleted;
                break;
            default:
                Tokenizer_.ThrowUnexpected();
        }
    }

    void OnMyEndMap() override
    {
        CheckAndThrowResolveError();
    }

    void OnMyBeginAttributes() override
    {
        if (State_ == EConsumerState::ConsumptionCompleted) {
            return;
        }

        Tokenizer_.Skip(NYPath::ETokenType::Slash);
        if (Tokenizer_.GetType() == NYPath::ETokenType::At) {
            Tokenizer_.Advance();
            if (Tokenizer_.GetType() == NYPath::ETokenType::EndOfStream) {
                UnderlyingConsumer_->OnBeginMap();
                Forward(
                    UnderlyingConsumer_,
                    [&] {
                        UnderlyingConsumer_->OnEndMap();
                        State_= EConsumerState::ConsumptionCompleted;
                    },
                    EYsonType::MapFragment);
            }
        } else {
            SkipSubTree(EYsonType::MapFragment);
        }
    }

    void OnMyEndAttributes() override
    { }

private:
    enum class EConsumerState
    {
        FollowingPath,
        ConsumptionCompleted,
    };

    const NYPath::TYPath Path_;
    const EMissingPathMode MissingPathMode_;

    NYPath::TTokenizer Tokenizer_;
    IYsonConsumer* UnderlyingConsumer_;
    TNullYsonConsumer NullConsumer_;
    EConsumerState State_ = EConsumerState::FollowingPath;
    int CurrentListItemIndex_ = -1;

    void ThrowResolveError()
    {
        THROW_ERROR_EXCEPTION(NYTree::EErrorCode::ResolveError, "Failed to resolve YPath")
            << TErrorAttribute("full_path", Tokenizer_.GetPath())
            << TErrorAttribute("resolved_prefix", Tokenizer_.GetPrefix());
    }

    void ThrowNoAttributes()
    {
        THROW_ERROR_EXCEPTION("Path %Qv has no attributes", Tokenizer_.GetPrefix());
    }

    void CheckAndThrowResolveError()
    {
        if (State_ == EConsumerState::FollowingPath) {
            if (MissingPathMode_ == EMissingPathMode::ThrowError) {
                ThrowResolveError();
            }
            State_ = EConsumerState::ConsumptionCompleted;
        }
    }

    void ForwardIfPathIsExhausted()
    {
        if (Tokenizer_.GetType() == NYPath::ETokenType::EndOfStream) {
            Forward(UnderlyingConsumer_, [&] { State_ = EConsumerState::ConsumptionCompleted; });
        }
    }

    void SkipSubTree(EYsonType type = EYsonType::Node)
    {
        Forward(&NullConsumer_, /*onFinished*/ nullptr, type);
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IYsonConsumer> CreateYPathDesignatedConsumer(
    NYPath::TYPath path,
    EMissingPathMode missingPathMode,
    IYsonConsumer* underlyingConsumer)
{
    return std::make_unique<TYPathDesignatedYsonConsumer>(
        std::move(path),
        missingPathMode,
        underlyingConsumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
