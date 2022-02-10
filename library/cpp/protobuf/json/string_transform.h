#pragma once

#include <library/cpp/string_utils/relaxed_escaper/relaxed_escaper.h>
#include <util/generic/ptr.h>
#include <util/generic/refcount.h>

namespace NProtobufJson {
    class IStringTransform: public TSimpleRefCount<IStringTransform> {
    public:
        virtual ~IStringTransform() {
        }

        /// Some transforms have special meaning.
        /// For example, escape transforms cause generic JSON escaping to be turned off.
        enum Type {
            EscapeTransform = 0x1,
        };

        virtual int GetType() const = 0;

        /// This method is called for each string field in proto
        virtual void Transform(TString& str) const = 0;

        /// This method is called for each bytes field in proto
        virtual void TransformBytes(TString& str) const {
            // Default behaviour is to apply string transform
            return Transform(str);
        }
    };

    using TStringTransformPtr = TIntrusivePtr<IStringTransform>;

    template <bool quote, bool tounicode>
    class TEscapeJTransform: public IStringTransform {
    public:
        int GetType() const override {
            return EscapeTransform;
        }

        void Transform(TString& str) const override {
            TString newStr;
            NEscJ::EscapeJ<quote, tounicode>(str, newStr);
            str = newStr;
        }
    };

    class TCEscapeTransform: public IStringTransform {
    public:
        int GetType() const override {
            return EscapeTransform;
        }

        void Transform(TString& str) const override;
    };

    class TSafeUtf8CEscapeTransform: public IStringTransform {
    public:
        int GetType() const override {
            return EscapeTransform;
        }

        void Transform(TString& str) const override;
    };

    class TDoubleEscapeTransform: public IStringTransform {
    public:
        int GetType() const override {
            return EscapeTransform;
        }

        void Transform(TString& str) const override;
    };

    class TDoubleUnescapeTransform: public NProtobufJson::IStringTransform {
    public:
        int GetType() const override {
            return NProtobufJson::IStringTransform::EscapeTransform;
        }

        void Transform(TString& str) const override;

    private:
        TString Unescape(const TString& str) const;
    };

    class TBase64EncodeBytesTransform: public NProtobufJson::IStringTransform {
    public:
        int GetType() const override {
            return 0;
        }

        void Transform(TString&) const override {
            // Do not transform strings
        }

        void TransformBytes(TString &str) const override;
    };

    class TBase64DecodeBytesTransform: public NProtobufJson::IStringTransform {
    public:
        int GetType() const override {
            return 0;
        }

        void Transform(TString&) const override {
            // Do not transform strings
        }

        void TransformBytes(TString &str) const override;
    };
}
