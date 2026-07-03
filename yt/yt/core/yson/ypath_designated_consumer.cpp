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
    , public IFlushableYsonConsumer
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
        InitTokenizer();
        ForwardIfPathIsExhausted();
    }

    void Flush() override
    {
        // Emit a single fallback entity if the path missed and the underlying
        // consumer hasn't received anything from this document yet.
        if (State_ != EConsumerState::ValueForwarded &&
            MissingPathMode_ == EMissingPathMode::EmitEntity)
        {
            UnderlyingConsumer_->OnEntity();
        }
        // Drop any pending forwarding state from the base class and reset our
        // own state so the consumer can be reused for the next YSON document.
        SetState({});
        State_ = EConsumerState::FollowingPath;
        CurrentListItemIndex_ = -1;
        Tokenizer_.Reset(Path_);
        InitTokenizer();
        ForwardIfPathIsExhausted();
    }

    void OnMyStringScalar(TStringBuf /*value*/) override
    {
        HandleMissingPath();
    }

    void OnMyInt64Scalar(i64 /*value*/) override
    {
        HandleMissingPath();
    }

    void OnMyUint64Scalar(ui64 /*value*/) override
    {
        HandleMissingPath();
    }

    void OnMyDoubleScalar(double /*value*/) override
    {
        HandleMissingPath();
    }

    void OnMyBooleanScalar(bool /*value*/) override
    {
        HandleMissingPath();
    }

    void OnMyEntity() override
    {
        HandleMissingPath();
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
        if (State_ != EConsumerState::FollowingPath) {
            return;
        }

        ++CurrentListItemIndex_;
        switch (Tokenizer_.GetType()) {
            case NYPath::ETokenType::Literal: {
                int indexInPath;
                if (!TryFromString(Tokenizer_.GetToken(), indexInPath)) {
                    HandleMissingPath();
                    return;
                }
                if (indexInPath == CurrentListItemIndex_) {
                    Tokenizer_.Advance();
                    ForwardIfPathIsExhausted();
                } else {
                    SkipSubTree();
                }
                break;
            }

            default:
                Tokenizer_.ThrowUnexpected();
        }
    }

    void OnMyEndList() override
    {
        HandleMissingPath();
    }

    void OnMyBeginMap() override
    {
        if (State_ == EConsumerState::FollowingPath) {
            Tokenizer_.Skip(NYPath::ETokenType::Slash);
        }
    }

    void OnMyKeyedItem(TStringBuf key) override
    {
        if (State_ != EConsumerState::FollowingPath) {
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
                State_ = EConsumerState::PathMissed;
                break;
            default:
                Tokenizer_.ThrowUnexpected();
        }
    }

    void OnMyEndMap() override
    {
        HandleMissingPath();
    }

    void OnMyBeginAttributes() override
    {
        if (State_ != EConsumerState::FollowingPath) {
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
                        State_ = EConsumerState::ValueForwarded;
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
        //! Still navigating along the path.
        FollowingPath,
        //! Path matched; the value has been (partially) forwarded.
        ValueForwarded,
        //! Path missed; no value will be emitted from the input itself.
        PathMissed,
    };

    const NYPath::TYPath Path_;
    const EMissingPathMode MissingPathMode_;

    NYPath::TTokenizer Tokenizer_;
    IYsonConsumer* const UnderlyingConsumer_;
    TNullYsonConsumer NullConsumer_;
    EConsumerState State_ = EConsumerState::FollowingPath;
    int CurrentListItemIndex_ = -1;

    void InitTokenizer()
    {
        Tokenizer_.Skip(NYPath::ETokenType::StartOfStream);
    }

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

    void HandleMissingPath()
    {
        if (State_ != EConsumerState::FollowingPath) {
            return;
        }
        if (MissingPathMode_ == EMissingPathMode::ThrowError) {
            ThrowResolveError();
        }
        State_ = EConsumerState::PathMissed;
    }

    void ForwardIfPathIsExhausted()
    {
        if (Tokenizer_.GetType() == NYPath::ETokenType::EndOfStream) {
            Forward(UnderlyingConsumer_, [&] { State_ = EConsumerState::ValueForwarded; });
        }
    }

    void SkipSubTree(EYsonType type = EYsonType::Node)
    {
        Forward(&NullConsumer_, /*onFinished*/ nullptr, type);
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IFlushableYsonConsumer> CreateYPathDesignatedConsumer(
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
