#include "stripped_error.h"

// TODO(arkady-e1ppa): core/misc deps can easily be moved
// to relevant library/cpp/yt sections.
#include <yt/yt/core/misc/string_helpers.h>
#include <yt/yt/core/misc/proc.h>

// TODO(arkady-e1ppa): core/ytree deps removal requires making
// a separate class that has some features of
// IAttributeDictionary. Namely, it needs to
// be created, have TYsonString added to it
// merged with other dictionary and deep copied.
// NB: We don't need printability since TError from
// this file is not printable.
#include <yt/yt/core/ytree/attributes.h>

#include <yt/yt/core/misc/origin_attributes.h>

#include <library/cpp/yt/exception/exception.h>

#include <library/cpp/yt/error/origin_attributes.h>

#include <library/cpp/yt/yson_string/string.h>

#include <util/string/subst.h>

#include <util/system/error.h>
#include <util/system/thread.h>

namespace NYT {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, TErrorCode code, TStringBuf spec)
{
    FormatValue(builder, static_cast<int>(code), spec);
}

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf ErrorMessageTruncatedSuffix = "...<message truncated>";

////////////////////////////////////////////////////////////////////////////////

class TError::TImpl
{
public:
    TImpl()
        : Code_(NYT::EErrorCode::OK)
    { }

    TImpl(const TError::TImpl& other)
        : Code_(other.Code_)
        , Message_(other.Message_)
        , OriginAttributes_(other.OriginAttributes_)
        , Attributes_(other.Attributes_ ? other.Attributes_->Clone() : nullptr)
        , InnerErrors_(other.InnerErrors_)
    { }

    explicit TImpl(TString message)
        : Code_(NYT::EErrorCode::Generic)
        , Message_(std::move(message))
    {
        OriginAttributes_.Capture();
    }

    TImpl(TErrorCode code, TString message)
        : Code_(code)
        , Message_(std::move(message))
    {
        if (!IsOK()) {
            OriginAttributes_.Capture();
        }
    }

    bool IsOK() const noexcept
    {
        return Code_ == NYT::EErrorCode::OK;
    }

    TErrorCode GetCode() const noexcept
    {
        return Code_;
    }

    void SetCode(TErrorCode code) noexcept
    {
        Code_ = code;
    }

    const TString& GetMessage() const noexcept
    {
        return Message_;
    }

    TString* MutableMessage() noexcept
    {
        return &Message_;
    }

    bool HasOriginAttributes() const noexcept
    {
        return OriginAttributes_.ThreadName.Length > 0;
    }

    const TOriginAttributes& OriginAttributes() const noexcept
    {
        return OriginAttributes_;
    }

    TOriginAttributes* MutableOriginAttributes() noexcept
    {
        return &OriginAttributes_;
    }

    TProcessId GetPid() const noexcept
    {
        return OriginAttributes_.Pid;
    }

    NThreading::TThreadId GetTid() const noexcept
    {
        return OriginAttributes_.Tid;
    }

    TStringBuf GetThreadName() const noexcept
    {
        return OriginAttributes_.ThreadName.ToStringBuf();
    }

    bool HasDatetime() const noexcept
    {
        return OriginAttributes_.Datetime != TInstant();
    }

    TInstant GetDatetime() const noexcept
    {
        return OriginAttributes_.Datetime;
    }

    void SetDatetime(TInstant datetime) noexcept
    {
        OriginAttributes_.Datetime = datetime;
    }

    bool HasAttributes() const noexcept
    {
        return Attributes_.operator bool();
    }

    const IAttributeDictionary& Attributes() const
    {
        if (!Attributes_) {
            return EmptyAttributes();
        }
        return *Attributes_;
    }

    void SetAttributes(IAttributeDictionaryPtr attributes)
    {
        Attributes_ = std::move(attributes);
        ExtractBultinAttributes();
    }

    IAttributeDictionary* MutableAttributes() noexcept
    {
        if (!Attributes_) {
            Attributes_ = CreateEphemeralAttributes();
        }
        return Attributes_.Get();
    }

    const std::vector<TError>& InnerErrors() const noexcept
    {
        return InnerErrors_;
    }

    std::vector<TError>* MutableInnerErrors() noexcept
    {
        return &InnerErrors_;
    }

private:
    TErrorCode Code_;
    TString Message_;

    TOriginAttributes OriginAttributes_{
        .Pid = 0,
        .Tid = NThreading::InvalidThreadId,
    };

    NYTree::IAttributeDictionaryPtr Attributes_;
    std::vector<TError> InnerErrors_;

    void ExtractBultinAttributes()
    {
        OriginAttributes_ = NYT::NDetail::ExtractFromDictionary(Attributes_);
    }
};

////////////////////////////////////////////////////////////////////////////////

namespace  {

bool IsWhitelisted(const TError& error, const THashSet<TStringBuf>& attributeWhitelist)
{
    for (const auto& key : error.Attributes().ListKeys()) {
        if (attributeWhitelist.contains(key)) {
            return true;
        }
    }

    for (const auto& innerError : error.InnerErrors()) {
        if (IsWhitelisted(innerError, attributeWhitelist)) {
            return true;
        }
    }

    return false;
}

//! Returns vector which consists of objects from errors such that:
//! if N is the number of objects in errors s.t. IsWhitelisted is true
//! then first N objects of returned vector are the ones for which IsWhitelisted is true
//! followed by std::max(0, maxInnerErrorCount - N - 1) remaining objects
//! finally followed by errors.back().
std::vector<TError>& ApplyWhitelist(std::vector<TError>& errors, const THashSet<TStringBuf>& attributeWhitelist, int maxInnerErrorCount)
{
    if (std::ssize(errors) < std::max(2, maxInnerErrorCount)) {
        return errors;
    }

    auto firstNotWhitelisted = std::partition(
        errors.begin(),
        std::prev(errors.end()),
        [&attributeWhitelist] (const TError& error) {
            return IsWhitelisted(error, attributeWhitelist);
        });

    int lastErrorOffset = std::max<int>(firstNotWhitelisted - errors.begin(), maxInnerErrorCount - 1);

    *(errors.begin() + lastErrorOffset) = std::move(errors.back());
    errors.resize(lastErrorOffset + 1);

    return errors;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TError::TErrorOr() = default;

TError::~TErrorOr() = default;

TError::TErrorOr(const TError& other)
{
    if (!other.IsOK()) {
        Impl_ = std::make_unique<TImpl>(*other.Impl_);
    }
}

TError::TErrorOr(TError&& other) noexcept
    : Impl_(std::move(other.Impl_))
{ }

TError::TErrorOr(const std::exception& ex)
{
    if (auto simpleException = dynamic_cast<const TSimpleException*>(&ex)) {
        *this = TError(NYT::EErrorCode::Generic, TRuntimeFormat{simpleException->GetMessage()});
        // NB: clang-14 is incapable of capturing structured binding variables
        //  so we force materialize them via this function call.
        auto addAttribute = [this] (const auto& key, const auto& value) {
            std::visit([&] (const auto& actual) {
                *this <<= TErrorAttribute(key, actual);
            }, value);
        };
        for (const auto& [key, value] : simpleException->GetAttributes()) {
            addAttribute(key, value);
        }
        try {
            if (simpleException->GetInnerException()) {
                std::rethrow_exception(simpleException->GetInnerException());
            }
        } catch (const std::exception& innerEx) {
            *this <<= TError(innerEx);
        }
    } else if (const auto* errorEx = dynamic_cast<const TErrorException*>(&ex)) {
        *this = errorEx->Error();
    } else {
        *this = TError(NYT::EErrorCode::Generic, TRuntimeFormat{ex.what()});
    }
    YT_VERIFY(!IsOK());
}

TError::TErrorOr(TString message, TDisableFormat)
    : Impl_(std::make_unique<TImpl>(std::move(message)))
{ }

TError::TErrorOr(TErrorCode code, TString message, TDisableFormat)
    : Impl_(std::make_unique<TImpl>(code, std::move(message)))
{ }

TError& TError::operator = (const TError& other)
{
    if (other.IsOK()) {
        Impl_.reset();
    } else {
        Impl_ = std::make_unique<TImpl>(*other.Impl_);
    }
    return *this;
}

TError& TError::operator = (TError&& other) noexcept
{
    Impl_ = std::move(other.Impl_);
    return *this;
}

TError TError::FromSystem()
{
    return FromSystem(LastSystemError());
}

TError TError::FromSystem(int error)
{
    return TError(TErrorCode(LinuxErrorCodeBase + error), TRuntimeFormat{LastSystemErrorText(error)}) <<
        TErrorAttribute("errno", error);
}

TError TError::FromSystem(const TSystemError& error)
{
    return FromSystem(error.Status());
}

TErrorCode TError::GetCode() const
{
    if (!Impl_) {
        return NYT::EErrorCode::OK;
    }
    return Impl_->GetCode();
}

TError& TError::SetCode(TErrorCode code)
{
    MakeMutable();
    Impl_->SetCode(code);
    return *this;
}

TErrorCode TError::GetNonTrivialCode() const
{
    if (!Impl_) {
        return NYT::EErrorCode::OK;
    }

    if (GetCode() != NYT::EErrorCode::Generic) {
        return GetCode();
    }

    for (const auto& innerError : InnerErrors()) {
        auto innerCode = innerError.GetNonTrivialCode();
        if (innerCode != NYT::EErrorCode::Generic) {
            return innerCode;
        }
    }

    return GetCode();
}

THashSet<TErrorCode> TError::GetDistinctNonTrivialErrorCodes() const
{
    THashSet<TErrorCode> result;
    TraverseError(*this, [&result] (const TError& error, int /*depth*/) {
        if (auto errorCode = error.GetCode(); errorCode != NYT::EErrorCode::OK) {
            result.insert(errorCode);
        }
    });
    return result;
}

const TString& TError::GetMessage() const
{
    if (!Impl_) {
        static const TString Result;
        return Result;
    }
    return Impl_->GetMessage();
}

TError& TError::SetMessage(TString message)
{
    MakeMutable();
    *Impl_->MutableMessage() = std::move(message);
    return *this;
}

bool TError::HasOriginAttributes() const
{
    if (!Impl_) {
        return false;
    }
    return Impl_->HasOriginAttributes();
}

bool TError::HasDatetime() const
{
    if (!Impl_) {
        return false;
    }
    return Impl_->HasDatetime();
}

TInstant TError::GetDatetime() const
{
    if (!Impl_) {
        return {};
    }
    return Impl_->GetDatetime();
}

TProcessId TError::GetPid() const
{
    if (!Impl_) {
        return 0;
    }
    return Impl_->GetPid();
}

NThreading::TThreadId TError::GetTid() const
{
    if (!Impl_) {
        return NThreading::InvalidThreadId;
    }
    return Impl_->GetTid();
}

TStringBuf TError::GetThreadName() const
{
    if (!Impl_) {
        static TString empty;
        return empty;
    }
    return Impl_->GetThreadName();
}

bool TError::HasAttributes() const noexcept
{
    if (!Impl_) {
        return false;
    }
    return Impl_->HasAttributes();
}

const IAttributeDictionary& TError::Attributes() const
{
    if (!Impl_) {
        return EmptyAttributes();
    }
    return Impl_->Attributes();
}

IAttributeDictionary* TError::MutableAttributes()
{
    MakeMutable();
    return Impl_->MutableAttributes();
}

const std::vector<TError>& TError::InnerErrors() const
{
    if (!Impl_) {
        static const std::vector<TError> Result;
        return Result;
    }
    return Impl_->InnerErrors();
}

std::vector<TError>* TError::MutableInnerErrors()
{
    MakeMutable();
    return Impl_->MutableInnerErrors();
}

TOriginAttributes* TError::MutableOriginAttributes() const noexcept
{
    if (!Impl_) {
        return nullptr;
    }
    return Impl_->MutableOriginAttributes();
}

void TError::SetAttributes(NYTree::IAttributeDictionaryPtr attributes)
{
    if (!Impl_) {
        return;
    }
    Impl_->SetAttributes(std::move(attributes));
}

const TString InnerErrorsTruncatedKey("inner_errors_truncated");

TError TError::Truncate(
    int maxInnerErrorCount,
    i64 stringLimit,
    const THashSet<TStringBuf>& attributeWhitelist) const &
{
    if (!Impl_) {
        return TError();
    }

    auto truncateInnerError = [=, &attributeWhitelist] (const TError& innerError) {
        return innerError.Truncate(maxInnerErrorCount, stringLimit, attributeWhitelist);
    };

    auto truncateAttributes = [stringLimit, &attributeWhitelist] (const IAttributeDictionary& attributes) {
        auto truncatedAttributes = CreateEphemeralAttributes();
        for (const auto& key : attributes.ListKeys()) {
            const auto& value = attributes.FindYson(key);

            if (std::ssize(value.AsStringBuf()) > stringLimit && !attributeWhitelist.contains(key)) {
                truncatedAttributes->SetYson(
                    key,
                    NYson::ConvertToYsonString("...<attribute truncated>..."));
            } else {
                truncatedAttributes->SetYson(
                    key,
                    value);
            }
        }
        return truncatedAttributes;
    };

    auto result = std::make_unique<TImpl>();
    result->SetCode(GetCode());
    *result->MutableMessage()
        = std::move(TruncateString(GetMessage(), stringLimit, ErrorMessageTruncatedSuffix));
    if (Impl_->HasAttributes()) {
        result->SetAttributes(truncateAttributes(Impl_->Attributes()));
    }
    *result->MutableOriginAttributes() = Impl_->OriginAttributes();

    const auto& innerErrors = InnerErrors();
    auto& copiedInnerErrors = *result->MutableInnerErrors();

    if (std::ssize(innerErrors) <= maxInnerErrorCount) {
        for (const auto& innerError : innerErrors) {
            copiedInnerErrors.push_back(truncateInnerError(innerError));
        }
    } else {
        result->MutableAttributes()->Set(InnerErrorsTruncatedKey, true);

        // NB(arkady-e1ppa): We want to always keep the last inner error,
        // so we make room for it and do not check if it is whitelisted.
        for (int idx = 0; idx < std::ssize(innerErrors) - 1; ++idx) {
            const auto& innerError = innerErrors[idx];
            if (
                IsWhitelisted(innerError, attributeWhitelist) ||
                std::ssize(copiedInnerErrors) < maxInnerErrorCount - 1)
            {
                copiedInnerErrors.push_back(truncateInnerError(innerError));
            }
        }
        copiedInnerErrors.push_back(truncateInnerError(innerErrors.back()));
    }

    return TError(std::move(result));
}

TError TError::Truncate(
    int maxInnerErrorCount,
    i64 stringLimit,
    const THashSet<TStringBuf>& attributeWhitelist) &&
{
    if (!Impl_) {
        return TError();
    }

    auto truncateInnerError = [=, &attributeWhitelist] (TError& innerError) {
        innerError = std::move(innerError).Truncate(maxInnerErrorCount, stringLimit, attributeWhitelist);
    };

    auto truncateAttributes = [stringLimit, &attributeWhitelist] (IAttributeDictionary* attributes) {
        for (const auto& key : attributes->ListKeys()) {
            if (std::ssize(attributes->FindYson(key).AsStringBuf()) > stringLimit && !attributeWhitelist.contains(key)) {
                attributes->SetYson(
                    key,
                    NYson::ConvertToYsonString("...<attribute truncated>..."));
            }
        }
    };

    TruncateStringInplace(Impl_->MutableMessage(), stringLimit, ErrorMessageTruncatedSuffix);
    if (Impl_->HasAttributes()) {
        truncateAttributes(Impl_->MutableAttributes());
    }
    if (std::ssize(InnerErrors()) <= maxInnerErrorCount) {
        for (auto& innerError : *MutableInnerErrors()) {
            truncateInnerError(innerError);
        }
    } else {
        auto& innerErrors = ApplyWhitelist(*MutableInnerErrors(), attributeWhitelist, maxInnerErrorCount);
        MutableAttributes()->Set(InnerErrorsTruncatedKey, true);

        for (auto& innerError : innerErrors) {
            truncateInnerError(innerError);
        }
    }

    return std::move(*this);
}

bool TError::IsOK() const
{
    if (!Impl_) {
        return true;
    }
    return Impl_->IsOK();
}

TError TError::Wrap() const &
{
    return *this;
}

TError TError::Wrap() &&
{
    return std::move(*this);
}

Y_WEAK TString GetErrorSkeleton(const TError& /*error*/)
{
    // Proper implementation resides in yt/yt/library/error_skeleton/skeleton.cpp.
    THROW_ERROR_EXCEPTION("Error skeleton implementation library is not linked; consider PEERDIR'ing yt/yt/library/error_skeleton");
}

TString TError::GetSkeleton() const
{
    return GetErrorSkeleton(*this);
}

std::optional<TError> TError::FindMatching(TErrorCode code) const
{
    return FindMatching([&] (TErrorCode errorCode) {
        return code == errorCode;
    });
}

std::optional<TError> TError::FindMatching(const THashSet<TErrorCode>& codes) const
{
    return FindMatching([&] (TErrorCode code) {
        return codes.contains(code);
    });
}

TError::TErrorOr(std::unique_ptr<TImpl> impl)
    : Impl_(std::move(impl))
{ }

void TError::MakeMutable()
{
    if (!Impl_) {
        Impl_ = std::make_unique<TImpl>();
    }
}

////////////////////////////////////////////////////////////////////////////////

TError& TError::operator <<= (const TErrorAttribute& attribute) &
{
    MutableAttributes()->SetYson(attribute.Key, attribute.Value);
    return *this;
}

TError& TError::operator <<= (const std::vector<TErrorAttribute>& attributes) &
{
    for (const auto& attribute : attributes) {
        MutableAttributes()->SetYson(attribute.Key, attribute.Value);
    }
    return *this;
}

TError& TError::operator <<= (const TError& innerError) &
{
    MutableInnerErrors()->push_back(innerError);
    return *this;
}

TError& TError::operator <<= (TError&& innerError) &
{
    MutableInnerErrors()->push_back(std::move(innerError));
    return *this;
}

TError& TError::operator <<= (const std::vector<TError>& innerErrors) &
{
    MutableInnerErrors()->insert(
        MutableInnerErrors()->end(),
        innerErrors.begin(),
        innerErrors.end());
    return *this;
}

TError& TError::operator <<= (std::vector<TError>&& innerErrors) &
{
    MutableInnerErrors()->insert(
        MutableInnerErrors()->end(),
        std::make_move_iterator(innerErrors.begin()),
        std::make_move_iterator(innerErrors.end()));
    return *this;
}

TError& TError::operator <<= (const NYTree::IAttributeDictionary& attributes) &
{
    MutableAttributes()->MergeFrom(attributes);
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

bool operator == (const TError& lhs, const TError& rhs)
{
    if (!lhs.MutableOriginAttributes() && !rhs.MutableOriginAttributes()) {
        return true;
    }
    // NB(arkady-e1ppa): Origin attributes equality comparison is
    // bit-wise but garbage bits are zeroed so it shouldn't matter.
    return
        lhs.GetCode() == rhs.GetCode() &&
        lhs.GetMessage() == rhs.GetMessage() &&
        *lhs.MutableOriginAttributes() == *rhs.MutableOriginAttributes() &&
        lhs.Attributes() == rhs.Attributes() &&
        lhs.InnerErrors() == rhs.InnerErrors();
}

////////////////////////////////////////////////////////////////////////////////

void AppendIndent(TStringBuilderBase* builer, int indent)
{
    builer->AppendChar(' ', indent);
}

void AppendAttribute(TStringBuilderBase* builder, const TString& key, const TString& value, int indent)
{
    AppendIndent(builder, indent + 4);
    if (!value.Contains('\n')) {
        builder->AppendFormat("%-15s %s", key, value);
    } else {
        builder->AppendString(key);
        TString indentedValue = "\n" + value;
        SubstGlobal(indentedValue, "\n", "\n" + TString{static_cast<size_t>(indent + 8), ' '});
        // Now first line in indentedValue is empty and every other line is indented by 8 spaces.
        builder->AppendString(indentedValue);
    }
    builder->AppendChar('\n');
}

void AppendError(TStringBuilderBase* builder, const TError& error, int indent)
{
    if (error.IsOK()) {
        builder->AppendString("OK");
        return;
    }

    AppendIndent(builder, indent);
    builder->AppendString(error.GetMessage());
    builder->AppendChar('\n');

    if (error.GetCode() != NYT::EErrorCode::Generic) {
        AppendAttribute(builder, "code", ToString(static_cast<int>(error.GetCode())), indent);
    }

    // Pretty-print origin.
    if (error.HasOriginAttributes()) {
        AppendAttribute(
            builder,
            "origin",
            NYT::NDetail::FormatOrigin(*error.MutableOriginAttributes()),
            indent);
    } else if (IsErrorSanitizerEnabled() && HasHost(error)) {
        AppendAttribute(
            builder,
            "host",
            ToString(GetHost(error)),
            indent);
    }

    if (error.HasDatetime()) {
        AppendAttribute(
            builder,
            "datetime",
            Format("%v", error.GetDatetime()),
            indent);
    }

    for (const auto& [key, value] : error.Attributes().ListPairs()) {
        TTokenizer tokenizer(value.AsStringBuf());
        YT_VERIFY(tokenizer.ParseNext());
        switch (tokenizer.GetCurrentType()) {
            case ETokenType::String:
                AppendAttribute(builder, key, TString(tokenizer.CurrentToken().GetStringValue()), indent);
                break;
            case ETokenType::Int64:
                AppendAttribute(builder, key, ToString(tokenizer.CurrentToken().GetInt64Value()), indent);
                break;
            case ETokenType::Uint64:
                AppendAttribute(builder, key, ToString(tokenizer.CurrentToken().GetUint64Value()), indent);
                break;
            case ETokenType::Double:
                AppendAttribute(builder, key, ToString(tokenizer.CurrentToken().GetDoubleValue()), indent);
                break;
            case ETokenType::Boolean:
                AppendAttribute(builder, key, TString(FormatBool(tokenizer.CurrentToken().GetBooleanValue())), indent);
                break;
            default:
                AppendAttribute(builder, key, ConvertToYsonString(value, EYsonFormat::Text).ToString(), indent);
                break;
        }
    }

    for (const auto& innerError : error.InnerErrors()) {
        builder->AppendChar('\n');
        AppendError(builder, innerError, indent + 2);
    }
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TError& error, TStringBuf /*spec*/)
{
    AppendError(builder, error, 0);
}

////////////////////////////////////////////////////////////////////////////////

void TraverseError(const TError& error, const TErrorVisitor& visitor, int depth)
{
    visitor(error, depth);
    for (const auto& inner : error.InnerErrors()) {
        TraverseError(inner, visitor, depth + 1);
    }
}

////////////////////////////////////////////////////////////////////////////////

const char* TErrorException::what() const noexcept
{
    if (CachedWhat_.empty()) {
        CachedWhat_ = ToString(Error_);
    }
    return CachedWhat_.data();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
