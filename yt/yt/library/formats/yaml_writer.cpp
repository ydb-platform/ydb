#include "yaml_writer.h"

#include "helpers.h"
#include "yaml_helpers.h"

#include <yt/yt/client/formats/config.h>

#include <contrib/libs/yaml/include/yaml.h>

namespace NYT::NFormats {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////

class TYamlWriter
    : public TFormatsConsumerBase
{
public:
    TYamlWriter(
        IOutputStream* output,
        NYson::EYsonType /*type*/,
        TYamlFormatConfigPtr config)
        : Output_(output)
        , Config_(config)
    {
        SafeInvoke(yaml_emitter_initialize, &Emitter_);
        yaml_emitter_set_output(&Emitter_, &WriteHandler, this);
        EmitEvent(yaml_stream_start_event_initialize, YAML_ANY_ENCODING);
    }

    void Flush() override
    {
        SafeInvoke(yaml_emitter_flush, &Emitter_);
    }

    void OnStringScalar(TStringBuf value) override
    {
        OnNodeEnter();
        // We try to emit a plain (unquoted) scalar if possible. It may be not possible
        // either because of YAML syntax restrictions (which will be handled by libyaml switching
        // to a quoted style automatically), or because the plain style would produce a scalar
        // which belongs to one of Core YAML schema regexps, making reasonable parsers interpret it as
        // int/float/bool/null instead of string.
        //
        // PyYAML and Go YAML parsers handle this issue by checking the type that would be deduced from
        // an unquoted representation and quoting the scalar if it would be interpreted as a non-string type.
        // We utilize the same approach here.

        auto plainYamlType = DeduceScalarTypeFromValue(value);
        auto desiredScalarStyle = YAML_ANY_SCALAR_STYLE;
        if (plainYamlType != EYamlScalarType::String) {
            desiredScalarStyle = YAML_DOUBLE_QUOTED_SCALAR_STYLE;
        }

        EmitEvent(
            yaml_scalar_event_initialize,
            /*anchor*/ nullptr,
            /*tag*/ nullptr,
            reinterpret_cast<const yaml_char_t*>(value.Data()),
            value.Size(),
            /*plain_implicit*/ 1,
            /*quoted_implicit*/ 1,
            desiredScalarStyle);
        OnNodeLeave();
    }

    void OnInt64Scalar(i64 value) override
    {
        OnNodeEnter();
        // Int64 scalars are always represented as plain (unquoted) YAML scalars.
        // Core YAML schema regexps ensures that they will be interpreted as integers
        // by all reasonable YAML parsers.
        // Cf. https://yaml.org/spec/1.2.2/#1032-tag-resolution
        char buf[64];
        auto length = IntToString<10>(value, buf, sizeof(buf));
        EmitEvent(
            yaml_scalar_event_initialize,
            /*anchor*/ nullptr,
            /*tag*/ nullptr,
            reinterpret_cast<yaml_char_t*>(buf),
            length,
            /*plain_implicit*/ 1,
            /*quoted_implicit*/ 0,
            YAML_PLAIN_SCALAR_STYLE);
        OnNodeLeave();
    }

    void OnUint64Scalar(ui64 value) override
    {
        OnNodeEnter();
        // Uint64 scalars are by default represented as plain (unquoted) YAML scalars,
        // similar to Int64 scalars (see the comment in OnInt64Scalar).
        // However, we optionally support a custom "!yt/uint64" tag to preserve the
        // information that the value is unsigned, which may be useful to control
        // signedness upon writing and for YT -> YAML -> YT roundtrip consistency.
        char buf[64];
        auto length = IntToString<10>(value, buf, sizeof(buf));

        // In libyaml API plainImplicit defines whether the writer omits the tag.
        bool plainImplicit = !Config_->WriteUintTag;

        EmitEvent(
            yaml_scalar_event_initialize,
            /*anchor*/ nullptr,
            /*tag*/ reinterpret_cast<const yaml_char_t*>(YTUintTag.data()),
            reinterpret_cast<yaml_char_t*>(buf),
            length,
            /*plain_implicit*/ plainImplicit,
            /*quoted_implicit*/ 0,
            YAML_PLAIN_SCALAR_STYLE);
        OnNodeLeave();
    }

    void OnDoubleScalar(double value) override
    {
        OnNodeEnter();
        // Double scalars are by default represented as plain (unquoted) YAML scalars,
        // similar to Int64 scalars (see the comment in OnInt64Scalar).

        char buf[512];
        auto length = DoubleToYamlString(value, buf, sizeof(buf));
        EmitEvent(
            yaml_scalar_event_initialize,
            /*anchor*/ nullptr,
            /*tag*/ reinterpret_cast<const yaml_char_t*>(YAML_FLOAT_TAG),
            reinterpret_cast<yaml_char_t*>(buf),
            length,
            /*plain_implicit*/ 1,
            /*quoted_implicit*/ 0,
            YAML_PLAIN_SCALAR_STYLE);
        OnNodeLeave();
    }

    void OnBooleanScalar(bool value) override
    {
        OnNodeEnter();
        static const std::string_view trueLiteral = "true";
        static const std::string_view falseLiteral = "false";
        const std::string_view& literal = value ? trueLiteral : falseLiteral;
        EmitEvent(
            yaml_scalar_event_initialize,
            /*anchor*/ nullptr,
            /*tag*/ reinterpret_cast<const yaml_char_t*>(YAML_BOOL_TAG),
            reinterpret_cast<yaml_char_t*>(const_cast<char*>(literal.data())),
            literal.size(),
            /*plain_implicit*/ 1,
            /*quoted_implicit*/ 0,
            YAML_PLAIN_SCALAR_STYLE);
        OnNodeLeave();
    }

    virtual void OnEntity() override
    {
        OnNodeEnter();
        static const std::string_view nullLiteral = "null";
        EmitEvent(
            yaml_scalar_event_initialize,
            /*anchor*/ nullptr,
            /*tag*/ reinterpret_cast<const yaml_char_t*>(YAML_NULL_TAG),
            reinterpret_cast<yaml_char_t*>(const_cast<char*>(nullLiteral.data())),
            nullLiteral.size(),
            /*plain_implicit*/ 1,
            /*quoted_implicit*/ 0,
            YAML_PLAIN_SCALAR_STYLE);
        OnNodeLeave();
    }

    virtual void OnBeginList() override
    {
        OnNodeEnter();
        EmitEvent(
            yaml_sequence_start_event_initialize,
            /*anchor*/ nullptr,
            /*tag*/ nullptr,
            /*implicit*/ 1,
            YAML_ANY_SEQUENCE_STYLE);
    }

    virtual void OnListItem() override
    { }

    virtual void OnEndList() override
    {
        EmitEvent(yaml_sequence_end_event_initialize);
        OnNodeLeave();
    }

    virtual void OnBeginMap() override
    {
        OnNodeEnter();
        EmitEvent(
            yaml_mapping_start_event_initialize,
            /*anchor*/ nullptr,
            /*tag*/ nullptr,
            /*implicit*/ 1,
            YAML_ANY_MAPPING_STYLE);
    }

    virtual void OnKeyedItem(TStringBuf key) override
    {
        OnStringScalar(key);
    }

    virtual void OnEndMap() override
    {
        EmitEvent(yaml_mapping_end_event_initialize);
        OnNodeLeave();
    }

    virtual void OnBeginAttributes() override
    {
        // NB: Node with attributes in YAML is represented as a yt/attrnode-tagged 2-item sequence.
        OnNodeEnter();
        EmitEvent(
            yaml_sequence_start_event_initialize,
            /*anchor*/ nullptr,
            /*tag*/ reinterpret_cast<const yaml_char_t*>(YTAttrNodeTag.data()),
            /*implicit*/ 0,
            YAML_ANY_SEQUENCE_STYLE);
        EmitEvent(
            yaml_mapping_start_event_initialize,
            /*anchor*/ nullptr,
            /*tag*/ nullptr,
            /*implicit*/ 1,
            YAML_ANY_MAPPING_STYLE);
    }

    virtual void OnEndAttributes() override
    {
        EmitEvent(yaml_mapping_end_event_initialize);
        ImmediatelyAfterAttributes_ = true;
        OnNodeLeave(/*isAttributes*/ true);
        DepthsWithPendingValueClosure_.push_back(CurrentDepth_);
    }

private:
    using TEmitterPtr = std::unique_ptr<yaml_emitter_t, decltype(&yaml_emitter_delete)>;
    using TEventPtr = std::unique_ptr<yaml_event_t, decltype(&yaml_event_delete)>;

    IOutputStream* Output_;
    TYamlFormatConfigPtr Config_;
    TLibYamlEmitter Emitter_;
    TError WriteError_;

    // Utilities for tracking the current depth and the stack of depths at which
    // we must perform extra sequence closure due to yt/attrnode-tagged 2-item sequence convention.

    //! The depth of the current node in the YSON tree.
    int CurrentDepth_ = 0;
    //! A stack of depths at which attributes are present.
    std::vector<int> DepthsWithPendingValueClosure_ = {-1};
    //! A flag indicating that the we are immediately after the OnEndAttributes() event.
    bool ImmediatelyAfterAttributes_ = false;

    static int WriteHandler(void* data, unsigned char* buffer, size_t size)
    {
        auto* yamlWriter = reinterpret_cast<TYamlWriter*>(data);
        auto* output = yamlWriter->Output_;

        try {
            output->Write(buffer, size);
        } catch (const std::exception& ex) {
            // We do not expect the write handler to be called after an error.
            YT_ASSERT(yamlWriter->WriteError_.IsOK());
            yamlWriter->WriteError_ = TError(ex);
            return 0;
        }
        return 1;
    }

    //! A wrapper around C-style libyaml API calls that return 0 on error which
    //! throws an exception in case of an error.
    int SafeInvoke(auto* method, auto... args)
    {
        int result = method(args...);
        if (result == 0) {
            ThrowError();
        }
        return result;
    }

    //! Throw an exception formed from the emitter state and possibly the exception
    //! caught in the last write handler call.
    void ThrowError()
    {
        // Unfortunately, libyaml may sometimes YAML_NO_ERROR. This may lead
        // to unclear exceptions during parsing.
        auto yamlErrorType = static_cast<EYamlErrorType>(Emitter_.error);
        auto error = TError("YAML emitter error: %v", Emitter_.problem)
            << TErrorAttribute("yaml_error_type", yamlErrorType);

        if (!WriteError_.IsOK()) {
            error <<= WriteError_;
        }

        THROW_ERROR error;
    }

    void EmitEvent(auto* eventInitializer, auto... args)
    {
        yaml_event_t event;
        // Event initializer is guaranteed to release all resources in case of an error.
        SafeInvoke(eventInitializer, &event, args...);
        SafeInvoke(yaml_emitter_emit, &Emitter_, &event);
    }

    void OnNodeEnter()
    {
        // If we are at the depth 0 and it is not a break between the root node attributes and the root node,
        // emit the document start event.
        if (CurrentDepth_ == 0 && !ImmediatelyAfterAttributes_) {
            EmitEvent(
                yaml_document_start_event_initialize,
                /*version_directive*/ nullptr,
                /*tag_directives_start*/ nullptr,
                /*tag_directives_end*/ nullptr,
                /*implicit*/ 1);
        }
        ++CurrentDepth_;
        ImmediatelyAfterAttributes_ = false;
    }

    void OnNodeLeave(bool isAttributes = false)
    {
        --CurrentDepth_;
        if (CurrentDepth_ == DepthsWithPendingValueClosure_.back()) {
            EmitEvent(yaml_sequence_end_event_initialize);
            DepthsWithPendingValueClosure_.pop_back();
        }
        if (isAttributes) {
            ImmediatelyAfterAttributes_ = true;
        }
        // If we are leaving the root node and it is not a break between the root node attributes and the root node,
        // emit the document end event.
        if (CurrentDepth_ == 0 && !isAttributes) {
            EmitEvent(yaml_document_end_event_initialize, /*implicit*/ 1);
        }
    }

    size_t DoubleToYamlString(double value, char* buf, size_t size)
    {
        // Extra care must be taken to handle non-finite values (NaN, Inf, -Inf),
        // and also to ensure that the resulting value cannot be parsed as an integer.
        // Both things are done similarly to the corresponding logic in the YSON writer.
        // Cf. NYson::TUncheckedYsonTokenWriter::WriteTextDouble.

        if (std::isfinite(value)) {
            auto length = FloatToString(value, buf, size);
            std::string_view str(buf, length);
            if (str.find('.') == std::string::npos && str.find('e') == std::string::npos) {
                YT_VERIFY(length + 1 <= size);
                buf[length++] = '.';
            }
            return length;
        } else {
            static const std::string_view nanLiteral = ".nan";
            static const std::string_view infLiteral = ".inf";
            static const std::string_view negativeInfLiteral = "-.inf";

            std::string_view str;
            if (std::isnan(value)) {
                str = nanLiteral;
            } else if (std::isinf(value) && value > 0) {
                str = infLiteral;
            } else {
                str = negativeInfLiteral;
            }
            YT_VERIFY(str.size() + 1 <= size);
            ::memcpy(buf, str.data(), str.size() + 1);
            return str.size();
        }
    }
};

std::unique_ptr<IFlushableYsonConsumer> CreateYamlWriter(
    IZeroCopyOutput* output,
    NYson::EYsonType type,
    TYamlFormatConfigPtr config)
{
    // Note that output gets narrowed to IOutputStream* as the currently used yaml library
    // interface is not zero-copy by its nature.
    return std::make_unique<TYamlWriter>(output, type, config);
}

////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
