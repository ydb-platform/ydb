#include "yaml_parser.h"

#include "yaml_helpers.h"

#include <yt/yt/client/formats/parser.h>

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/misc/coro_pipe.h>

#include <contrib/libs/yaml/include/yaml.h>

namespace NYT::NFormats {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

//! A helper class that takes care of the repeated parts of a YAML document that
//! are expressed as anchors and aliases. Under the hood, materializes a YSON
//! string for each anchor and emits it to the underlying consumer via
//! OnRaw when needed.
/*!
 *  Implementation notes:
 *  - Conforming to YAML 1.2, alias may refer only to a previously defined anchor.
 *  - Aliasing an anchor to an ancestor node is not supported as the resulting document
 *    cannot be represent as a finite YSON (even though some implementations with tree
 *    representations support that, e.g. PyYAML).
 *  - According to the YAML spec, alias may be "overridden" by a later definition.
 *    This feature is considered error-prone, will probably be removed in next
 *    versions of YAML spec (https://github.com/yaml/yaml-spec/pull/65) and is not
 *    supported by us.
 *  - Using an alias to a scalar anchor as a map key or anchoring a map key are not
 *    supported for the sake of simpler implementation (and are considered a weird thing
 *    to do by an author of this code).
 */
class TAnchorRecordingConsumer
    : public IYsonConsumer
{
public:
    explicit TAnchorRecordingConsumer(IYsonConsumer* underlyingConsumer)
        : UnderlyingConsumer_(underlyingConsumer)
        , RunListWriter_(&RunListStream_, EYsonType::ListFragment)
    { }

    void OnStringScalar(TStringBuf value) override
    {
        ForAllConsumers([=] (auto* consumer) { consumer->OnStringScalar(value); });
        MaybeFinishAnchor();
    }

    void OnInt64Scalar(i64 value) override
    {
        ForAllConsumers([=] (auto* consumer) { consumer->OnInt64Scalar(value); });
        MaybeFinishAnchor();
    }

    void OnUint64Scalar(ui64 value) override
    {
        ForAllConsumers([=] (auto* consumer) { consumer->OnUint64Scalar(value); });
        MaybeFinishAnchor();
    }

    void OnDoubleScalar(double value) override
    {
        ForAllConsumers([=] (auto* consumer) { consumer->OnDoubleScalar(value); });
        MaybeFinishAnchor();
    }

    void OnBooleanScalar(bool value) override
    {
        ForAllConsumers([=] (auto* consumer) { consumer->OnBooleanScalar(value); });
        MaybeFinishAnchor();
    }

    void OnEntity() override
    {
        ForAllConsumers([=] (auto* consumer) { consumer->OnEntity(); });
        MaybeFinishAnchor();
    }

    void OnBeginList() override
    {
        ++CurrentDepth_;
        ForAllConsumers([=] (auto* consumer) { consumer->OnBeginList(); });
    }

    void OnListItem() override
    {
        ForAllConsumers([=] (auto* consumer) { consumer->OnListItem(); });
    }

    void OnEndList() override
    {
        ForAllConsumers([=] (auto* consumer) { consumer->OnEndList(); });
        --CurrentDepth_;
        MaybeFinishAnchor();
    }

    void OnBeginMap() override
    {
        ++CurrentDepth_;
        ForAllConsumers([] (auto* consumer) { consumer->OnBeginMap(); });
    }

    void OnKeyedItem(TStringBuf key) override
    {
        ForAllConsumers([=] (auto* consumer) { consumer->OnKeyedItem(key); });
    }

    void OnEndMap() override
    {
        ForAllConsumers([] (auto* consumer) { consumer->OnEndMap(); });
        --CurrentDepth_;
        MaybeFinishAnchor();
    }

    void OnBeginAttributes() override
    {
        ++CurrentDepth_;
        ForAllConsumers([] (auto* consumer) { consumer->OnBeginAttributes(); });
    }

    void OnEndAttributes() override
    {
        ForAllConsumers([] (auto* consumer) { consumer->OnEndAttributes(); });
        --CurrentDepth_;
        // NB: Do not call MaybeFinishAnchorOrRun here, as we do not want to record only
        // attribute map part of the node.
    }

    void OnRaw(TStringBuf yson, EYsonType type) override
    {
        // The only caller for this OnRaw is ourselves in case of aliases, and aliases
        // always point to YSON node.
        YT_VERIFY(type == EYsonType::Node);
        ForAllConsumers([=] (auto* consumer) { consumer->OnRaw(yson, type); });
        MaybeFinishAnchor();
    }

    void OnAnchor(const std::string& anchor)
    {
        StartRun();
        auto inserted = KnownAnchorNames_.insert(anchor).second;
        if (!inserted) {
            THROW_ERROR_EXCEPTION("Anchor %Qv is already defined", anchor);
        }
        auto& currentAnchor = ConstructingAnchors_.emplace_back();
        currentAnchor = {
            anchor,
            CurrentDepth_,
            GetCurrentRunOffset(),
        };
    }

    void OnAlias(const std::string& alias)
    {
        auto it = FinishedAnchors_.find(alias);
        if (it == FinishedAnchors_.end()) {
            THROW_ERROR_EXCEPTION("Alias %Qv refers to an undefined or unfinished anchor", alias);
        }
        auto& anchor = it->second;

        RunListWriter_.Flush();
        std::string_view yson = RunListStream_.Str();
        yson = yson.substr(anchor.StartOffset, anchor.EndOffset - anchor.StartOffset);
        // NB: TBufferedBinaryYsonWriter writes ';' in a bit different way that you would expect from
        // IYsonConsumer interface -- not as a reaction to OnListItem or OnKeyedItem, but rather when a node
        // is finished. This leads to yson string above always containing extra trailing ';'.
        // We strip it off, as our YSON consumers expect a YSON node to be serialized during OnAlias call.
        YT_VERIFY(yson.ends_with(NYson::NDetail::ItemSeparatorSymbol));
        yson.remove_suffix(1);
        OnRaw(yson, EYsonType::Node);
    }

private:
    IYsonConsumer* UnderlyingConsumer_;
    //! Whenever there is at least one anchor being recorded, the stream is used to
    //! record the YSON representation of the outermost anchor. We call the representation
    //! of such an outermost anchor a run. Conveniently, we represent runs as elements of
    //! a fictional YSON list, making each anchor a substring of that YSON list.
    TStringStream RunListStream_;
    TBufferedBinaryYsonWriter RunListWriter_;

    struct TAnchor
    {
        std::string Name;
        int Depth;
        ssize_t StartOffset;
        ssize_t EndOffset = -1;
    };
    //! A stack of all anchors currently being constructed.
    std::vector<TAnchor> ConstructingAnchors_;
    //! A set of all anchors that are currently constructed.
    THashSet<std::string> KnownAnchorNames_;

    //! A map containing YSON representations of anchors that have been finished.
    THashMap<std::string, TAnchor> FinishedAnchors_;

    int CurrentDepth_ = 0;

    void ForAllConsumers(auto&& action)
    {
        action(UnderlyingConsumer_);
        if (IsInRun()) {
            action(&RunListWriter_);
        }
    }

    void StartRun()
    {
        if (!IsInRun()) {
            RunListWriter_.OnListItem();
        }
    }

    bool IsInRun() const
    {
        return !ConstructingAnchors_.empty();
    }

    ssize_t GetCurrentRunOffset() const
    {
        YT_VERIFY(IsInRun());
        return RunListWriter_.GetTotalWrittenSize();
    }

    //! Checks current depth, maybe finalizes the innermost anchor, and if
    //! the anchor stack vanishes, finalizes the outermost anchor.
    void MaybeFinishAnchor()
    {
        if (!ConstructingAnchors_.empty() && CurrentDepth_ == ConstructingAnchors_.back().Depth) {
            // Finalize the innermost (stack topmost) anchor.
            YT_VERIFY(IsInRun());
            auto& anchor = ConstructingAnchors_.back();

            anchor.EndOffset = GetCurrentRunOffset();
            auto inserted = FinishedAnchors_.emplace(anchor.Name, std::move(anchor)).second;
            // Insertion is ensured by the checks in OnAnchor.
            YT_VERIFY(inserted);
            ConstructingAnchors_.pop_back();
        }
    }
};

class TYamlParser
{
public:
    TYamlParser(IInputStream* input, IYsonConsumer* consumer, TYamlFormatConfigPtr config, EYsonType ysonType)
        : Input_(input)
        , Consumer_(consumer)
        , Config_(std::move(config))
        , YsonType_(ysonType)
    {
        yaml_parser_initialize(&Parser_);
        yaml_parser_set_input(&Parser_, &ReadHandler, this);
    }

    void Parse()
    {
        VisitStream();
    }

private:
    IInputStream* Input_;
    TAnchorRecordingConsumer Consumer_;
    TYamlFormatConfigPtr Config_;
    EYsonType YsonType_;

    TLibYamlParser Parser_;

    TError ReadError_;

    TLibYamlEvent Event_;

    //! Convenience helper to get rid of the ugly casts.
    EYamlEventType GetEventType() const
    {
        return static_cast<EYamlEventType>(Event_.type);
    }

    static int ReadHandler(void* data, unsigned char* buffer, size_t size, size_t* sizeRead)
    {
        auto* yamlParser = reinterpret_cast<TYamlParser*>(data);
        auto* input = yamlParser->Input_;

        try {
            // IInputStream is similar to yaml_read_handler_t interface
            // in EOF case: former returns 0 from Read(), and latter
            // expects handler to set size_read to 0 and return 1
            *sizeRead = input->Read(buffer, size);
            return 1;
        } catch (const std::exception& ex) {
            // We do not expect the read handler to be called after an error.
            YT_ASSERT(yamlParser->ReadError_.IsOK());
            yamlParser->ReadError_ = TError(ex);
            // Not really used by libyaml, but let's set it to 0 just in case.
            *sizeRead = 0;
            return 0;
        }
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
    [[noreturn]] void ThrowError()
    {
        // Unfortunately, libyaml may sometimes set error = YAML_NO_ERROR. This may lead
        // to unclear exceptions during parsing.
        auto yamlErrorType = static_cast<EYamlErrorType>(Parser_.error);
        auto error = TError("YAML parser error: %v", Parser_.problem)
            << TErrorAttribute("yaml_error_type", yamlErrorType)
            << TErrorAttribute("problem_offset", Parser_.problem_offset)
            << TErrorAttribute("problem_value", Parser_.problem_value)
            << TErrorAttribute("problem_mark", Parser_.problem_mark);
        if (Parser_.context) {
            error <<= TErrorAttribute("context", Parser_.context);
            error <<= TErrorAttribute("context_mark", Parser_.context_mark);
        }
        if (!ReadError_.IsOK()) {
            error <<= ReadError_;
        }

        THROW_ERROR error;
    }

    //! Pull the next event from the parser into Event_ and check that it is one of the expected types.
    void PullEvent(std::initializer_list<EYamlEventType> expectedTypes)
    {
        Event_.Reset();
        SafeInvoke(yaml_parser_parse, &Parser_, &Event_);
        for (const auto expectedType : expectedTypes) {
            if (GetEventType() == expectedType) {
                return;
            }
        }
        // TODO(max42): stack and position!
        THROW_ERROR_EXCEPTION(
            "Unexpected event type %Qlv, expected one of %Qlv",
            GetEventType(),
            std::vector(expectedTypes));
    }

    void VisitStream()
    {
        PullEvent({EYamlEventType::StreamStart});
        while (true) {
            PullEvent({EYamlEventType::DocumentStart, EYamlEventType::StreamEnd});
            if (GetEventType() == EYamlEventType::StreamEnd) {
                break;
            }
            if (YsonType_ == EYsonType::ListFragment) {
                Consumer_.OnListItem();
            }
            VisitDocument();
        }
    }

    void VisitDocument()
    {
        PullEvent({
            EYamlEventType::Scalar,
            EYamlEventType::SequenceStart,
            EYamlEventType::MappingStart,
            EYamlEventType::Alias,
        });
        VisitNode();
        PullEvent({EYamlEventType::DocumentEnd});
    }

    void VisitNode()
    {
        auto maybeOnAnchor = [&] (yaml_char_t* anchor) {
            if (anchor) {
                Consumer_.OnAnchor(std::string(YamlLiteralToStringView(anchor)));
            }
        };
        switch (GetEventType()) {
            case EYamlEventType::Scalar:
                maybeOnAnchor(Event_.data.scalar.anchor);
                VisitScalar();
                break;
            case EYamlEventType::SequenceStart:
                maybeOnAnchor(Event_.data.scalar.anchor);
                VisitSequence();
                break;
            case EYamlEventType::MappingStart:
                maybeOnAnchor(Event_.data.scalar.anchor);
                VisitMapping(/*isAttributes*/ false);
                break;
            case EYamlEventType::Alias:
                Consumer_.OnAlias(std::string(YamlLiteralToStringView(Event_.data.alias.anchor)));
                break;
            default:
                YT_ABORT();
        }
    }

    void VisitScalar()
    {
        auto scalar = Event_.data.scalar;
        auto yamlValue = YamlLiteralToStringView(scalar.value, scalar.length);

        // According to YAML spec, there are two non-specific tags "!" and "?", and all other
        // tags are specific.
        //
        // If the tag is missing, parser should assign tag "!" to non-plain (quoted) scalars,
        // and "?" to plain scalars and collection nodes. For some reason, libyaml does not
        // do that for us.
        //
        // Then, "!"-tagged scalars should always be treated as strings, i.e. "!" -> YT string.
        //
        // Specific tags are either recognized by us, in which case we deduce a corresponding YT type,
        // or we assign a string type otherwise.
        //
        // For the "?"-tagged scalars we perform the type deduction based on the scalar value
        // (which is the most often case, as almost nobody uses type tags in YAML).
        //
        // Cf. https://yaml.org/spec/1.2.2/#332-resolved-tags
        std::string_view tag;
        if (scalar.tag) {
            tag = YamlLiteralToStringView(scalar.tag);
        } else if (scalar.style != YAML_PLAIN_SCALAR_STYLE) {
            tag = "!";
        } else {
            tag = "?";
        }

        EYamlScalarType yamlType;
        if (tag != "?") {
            yamlType = DeduceScalarTypeFromTag(tag);
        } else {
            yamlType = DeduceScalarTypeFromValue(yamlValue);
        }
        auto [ytType, nonStringScalar] = ParseScalarValue(yamlValue, yamlType);
        switch (ytType) {
            case ENodeType::String:
                Consumer_.OnStringScalar(yamlValue);
                break;
            case ENodeType::Int64:
                Consumer_.OnInt64Scalar(nonStringScalar.Int64);
                break;
            case ENodeType::Uint64:
                Consumer_.OnUint64Scalar(nonStringScalar.Uint64);
                break;
            case ENodeType::Double:
                Consumer_.OnDoubleScalar(nonStringScalar.Double);
                break;
            case ENodeType::Boolean:
                Consumer_.OnBooleanScalar(nonStringScalar.Boolean);
                break;
            case ENodeType::Entity:
                Consumer_.OnEntity();
                break;
            default:
                YT_ABORT();
        }
    }

    void VisitSequence()
    {
        // NB: YSON node with attributes is represented as a yt/attrnode-tagged YAML sequence,
        // so handle it as a special case.
        if (YamlLiteralToStringView(Event_.data.mapping_start.tag) == YTAttrNodeTag) {
            VisitNodeWithAttributes();
            return;
        }

        Consumer_.OnBeginList();
        while (true) {
            PullEvent({
                EYamlEventType::SequenceEnd,
                EYamlEventType::SequenceStart,
                EYamlEventType::MappingStart,
                EYamlEventType::Scalar,
                EYamlEventType::Alias
            });
            if (GetEventType() == EYamlEventType::SequenceEnd) {
                break;
            }
            Consumer_.OnListItem();
            VisitNode();
        }
        Consumer_.OnEndList();
    }

    void VisitNodeWithAttributes()
    {
        PullEvent({EYamlEventType::MappingStart});
        VisitMapping(/*isAttributes*/ true);

        PullEvent({
            EYamlEventType::Scalar,
            EYamlEventType::SequenceStart,
            EYamlEventType::MappingStart,
            EYamlEventType::Alias,
        });
        VisitNode();

        PullEvent({EYamlEventType::SequenceEnd});
    }

    void VisitMapping(bool isAttributes)
    {
        isAttributes ? Consumer_.OnBeginAttributes() : Consumer_.OnBeginMap();
        while (true) {
            PullEvent({
                EYamlEventType::MappingEnd,
                EYamlEventType::Scalar,
                // Yes, YAML is weird enough to support aliases as keys!
                EYamlEventType::Alias,
            });
            if (GetEventType() == EYamlEventType::MappingEnd) {
                break;
            } else if (GetEventType() == EYamlEventType::Alias) {
                THROW_ERROR_EXCEPTION("Using alias as a map key is not supported");
            } else {
                if (Event_.data.scalar.anchor) {
                    THROW_ERROR_EXCEPTION("Putting anchors on map keys is not supported");
                }
                auto key = YamlLiteralToStringView(Event_.data.scalar.value, Event_.data.scalar.length);
                Consumer_.OnKeyedItem(key);
            }

            PullEvent({
                EYamlEventType::Scalar,
                EYamlEventType::SequenceStart,
                EYamlEventType::MappingStart,
                EYamlEventType::Alias,
            });
            VisitNode();
        }
        isAttributes ? Consumer_.OnEndAttributes() : Consumer_.OnEndMap();
    }
};

void ParseYaml(
    IInputStream* input,
    IYsonConsumer* consumer,
    TYamlFormatConfigPtr config,
    EYsonType ysonType)
{
    TYamlParser parser(input, consumer, config, ysonType);
    parser.Parse();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
