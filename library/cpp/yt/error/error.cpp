#include "error.h"

#include <library/cpp/yt/exception/exception.h>

#include <library/cpp/yt/error/error_attributes.h>
#include <library/cpp/yt/error/origin_attributes.h>

#include <library/cpp/yt/string/string.h>

#include <library/cpp/yt/system/proc.h>

#include <library/cpp/yt/yson_string/string.h>

#include <util/string/subst.h>

#include <util/system/error.h>
#include <util/system/thread.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, TErrorCode code, TStringBuf spec)
{
    FormatValue(builder, static_cast<int>(code), spec);
}

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf ErrorMessageTruncatedSuffix = "...<message truncated>";

TError::TEnricher TError::Enricher_;
TError::TFromExceptionEnricher TError::FromExceptionEnricher_;

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
        , Attributes_(other.Attributes_)
        , InnerErrors_(other.InnerErrors_)
    { }

    explicit TImpl(std::string message)
        : Code_(NYT::EErrorCode::Generic)
        , Message_(std::move(message))
    {
        OriginAttributes_.Capture();
    }

    TImpl(TErrorCode code, std::string message)
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

    const std::string& GetMessage() const noexcept
    {
        return Message_;
    }

    std::string* MutableMessage() noexcept
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
        return true;
    }

    const TErrorAttributes& Attributes() const
    {
        return Attributes_;
    }

    void UpdateOriginAttributes()
    {
        OriginAttributes_ = NYT::NDetail::ExtractFromDictionary(&Attributes_);
    }

    TErrorAttributes* MutableAttributes() noexcept
    {
        return &Attributes_;
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
    std::string Message_;

    TOriginAttributes OriginAttributes_{
        .Pid = 0,
        .Tid = NThreading::InvalidThreadId,
    };

    TErrorAttributes Attributes_;

    std::vector<TError> InnerErrors_;
};

////////////////////////////////////////////////////////////////////////////////

namespace {

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

TError::TErrorOr(const TErrorException& errorEx) noexcept
{
    *this = errorEx.Error();
    // NB: TErrorException verifies that error not IsOK at throwing end.
    EnrichFromException(errorEx);
}

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
        *this <<= TErrorAttribute("exception_type", TypeName(ex));
    }
    EnrichFromException(ex);
    YT_VERIFY(!IsOK());
}

TError::TErrorOr(std::string message, TDisableFormat)
    : Impl_(std::make_unique<TImpl>(std::move(message)))
{
    Enrich();
}

TError::TErrorOr(TErrorCode code, std::string message, TDisableFormat)
    : Impl_(std::make_unique<TImpl>(code, std::move(message)))
{
    Enrich();
}

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

const std::string& TError::GetMessage() const
{
    if (!Impl_) {
        static const std::string Result;
        return Result;
    }
    return Impl_->GetMessage();
}

TError& TError::SetMessage(std::string message)
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
        static std::string empty;
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

const TErrorAttributes& TError::Attributes() const
{
    if (!Impl_) {
        static TErrorAttributes empty;
        return empty;
    }
    return Impl_->Attributes();
}

TErrorAttributes* TError::MutableAttributes()
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

void TError::UpdateOriginAttributes()
{
    if (!Impl_) {
        return;
    }
    Impl_->UpdateOriginAttributes();
}

const std::string InnerErrorsTruncatedKey("inner_errors_truncated");

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

    auto truncateAttributes = [stringLimit, &attributeWhitelist] (const TErrorAttributes& attributes, TErrorAttributes* mutableAttributes) {
        for (const auto& [key, value] : attributes.ListPairs()) {
            if (std::ssize(value) > stringLimit && !attributeWhitelist.contains(key)) {
                mutableAttributes->SetValue(
                    key,
                    NYT::ToErrorAttributeValue("...<attribute truncated>..."));
            } else {
                mutableAttributes->SetValue(
                    key,
                    value);
            }
        }
    };

    auto result = std::make_unique<TImpl>();
    result->SetCode(GetCode());
    *result->MutableMessage() = TruncateString(GetMessage(), stringLimit, ErrorMessageTruncatedSuffix);
    if (Impl_->HasAttributes()) {
        truncateAttributes(Impl_->Attributes(), result->MutableAttributes());
    }
    *result->MutableOriginAttributes() = Impl_->OriginAttributes();

    const auto& innerErrors = InnerErrors();
    auto& copiedInnerErrors = *result->MutableInnerErrors();

    if (std::ssize(innerErrors) <= maxInnerErrorCount) {
        for (const auto& innerError : innerErrors) {
            copiedInnerErrors.push_back(truncateInnerError(innerError));
        }
    } else {
        result->MutableAttributes()->SetValue(InnerErrorsTruncatedKey, NYT::ToErrorAttributeValue(true));

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

    auto truncateAttributes = [stringLimit, &attributeWhitelist] (TErrorAttributes* attributes) {
        for (const auto& [key, value] : attributes->ListPairs()) {
            if (std::ssize(value) > stringLimit && !attributeWhitelist.contains(key)) {
                attributes->SetValue(
                    key,
                    NYT::ToErrorAttributeValue("...<attribute truncated>..."));
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
        MutableAttributes()->SetValue(InnerErrorsTruncatedKey, NYT::ToErrorAttributeValue(true));

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

Y_WEAK std::string GetErrorSkeleton(const TError& /*error*/)
{
    // Proper implementation resides in yt/yt/library/error_skeleton/skeleton.cpp.
    THROW_ERROR_EXCEPTION("Error skeleton implementation library is not linked; consider PEERDIR'ing yt/yt/library/error_skeleton");
}

std::string TError::GetSkeleton() const
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

void TError::RegisterEnricher(TEnricher enricher)
{
    // NB: This daisy-chaining strategy is optimal when there's O(1) callbacks. Convert to a vector
    // if the number grows.
    if (!Enricher_) {
        Enricher_ = std::move(enricher);
        return;
    }
    Enricher_ = [first = std::move(Enricher_), second = std::move(enricher)] (TError* error) {
        first(error);
        second(error);
    };
}

void TError::RegisterFromExceptionEnricher(TFromExceptionEnricher enricher)
{
    // NB: This daisy-chaining strategy is optimal when there's O(1) callbacks. Convert to a vector
    // if the number grows.
    if (!FromExceptionEnricher_) {
        FromExceptionEnricher_ = std::move(enricher);
        return;
    }
    FromExceptionEnricher_ = [
        first = std::move(FromExceptionEnricher_),
        second = std::move(enricher)
    ] (TError* error, const std::exception& exception) {
        first(error, exception);
        second(error, exception);
    };
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

void TError::Enrich()
{
    if (Enricher_) {
        Enricher_(this);
    }
}

void TError::EnrichFromException(const std::exception& exception)
{
    if (FromExceptionEnricher_) {
        FromExceptionEnricher_(this, exception);
    }
}

////////////////////////////////////////////////////////////////////////////////

TError& TError::operator <<= (const TErrorAttribute& attribute) &
{
    MutableAttributes()->SetAttribute(attribute);
    return *this;
}

TError& TError::operator <<= (const std::vector<TErrorAttribute>& attributes) &
{
    for (const auto& attribute : attributes) {
        MutableAttributes()->SetAttribute(attribute);
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

TError& TError::operator <<= (TAnyMergeableDictionaryRef attributes) &
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

void AppendAttribute(TStringBuilderBase* builder, const std::string& key, const std::string& value, int indent)
{
    AppendIndent(builder, indent + 4);
    if (value.find('\n') == std::string::npos) {
        builder->AppendFormat("%-15s %s", key, value);
    } else {
        builder->AppendString(key);
        std::string indentedValue = "\n" + value;
        SubstGlobal(indentedValue, "\n", "\n" + std::string(static_cast<size_t>(indent + 8), ' '));
        // Now first line in indentedValue is empty and every other line is indented by 8 spaces.
        builder->AppendString(indentedValue);
    }
    builder->AppendChar('\n');
}

void AppendError(TStringBuilderBase* builder, const TError& error, int indent)
{
    auto isStringTextYson = [] (TStringBuf str) {
        return
            str &&
            std::ssize(str) != 0 &&
            str.front() == '\"';
    };
    auto isBoolTextYson = [] (TStringBuf str) {
        return
            str == "%false" ||
            str == "%true";
    };

    if (error.IsOK()) {
        builder->AppendString("OK");
        return;
    }

    AppendIndent(builder, indent);
    builder->AppendString(error.GetMessage());
    builder->AppendChar('\n');

    if (error.GetCode() != NYT::EErrorCode::Generic) {
        AppendAttribute(builder, "code", NYT::ToString(static_cast<int>(error.GetCode())), indent);
    }

    // Pretty-print origin.
    const auto* originAttributes = error.MutableOriginAttributes();
    YT_ASSERT(originAttributes);
    if (error.HasOriginAttributes()) {
        AppendAttribute(
            builder,
            "origin",
            NYT::NDetail::FormatOrigin(*originAttributes),
            indent);
    } else if (IsErrorSanitizerEnabled() && originAttributes->Host.operator bool()) {
        AppendAttribute(
            builder,
            "host",
            std::string{originAttributes->Host},
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
        if (isStringTextYson(value)) {
            AppendAttribute(builder, key, NDetail::ConvertFromTextYsonString<std::string>(value), indent);
        } else if (isBoolTextYson(value)) {
            AppendAttribute(builder, key, std::string(FormatBool(NDetail::ConvertFromTextYsonString<bool>(value))), indent);
        } else {
            AppendAttribute(builder, key, value, indent);
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
