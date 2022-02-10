#include "legacy_protobuf.h"

#include <library/cpp/monlib/encode/legacy_protobuf/protos/metric_meta.pb.h>
#include <library/cpp/monlib/metrics/metric_consumer.h>
#include <library/cpp/monlib/metrics/labels.h>

#include <util/generic/yexception.h>
#include <util/generic/maybe.h>
#include <util/datetime/base.h>
#include <util/string/split.h>

#include <google/protobuf/reflection.h>

#include <algorithm>

#ifdef LEGACY_PB_TRACE
#define TRACE(msg) \
    Cerr << msg << Endl
#else
#define TRACE(...) ;
#endif

namespace NMonitoring {
    namespace {
        using TMaybeMeta = TMaybe<NMonProto::TMetricMeta>;

        TString ReadLabelValue(const NProtoBuf::Message& msg, const NProtoBuf::FieldDescriptor* d, const NProtoBuf::Reflection& r) {
            using namespace NProtoBuf;

            switch (d->type()) {
                case FieldDescriptor::TYPE_UINT32:
                    return ::ToString(r.GetUInt32(msg, d));
                case FieldDescriptor::TYPE_UINT64:
                    return ::ToString(r.GetUInt64(msg, d));
                case FieldDescriptor::TYPE_STRING:
                    return r.GetString(msg, d);
                case FieldDescriptor::TYPE_ENUM: {
                    auto val = r.GetEnumValue(msg, d);
                    auto* valDesc = d->enum_type()->FindValueByNumber(val);
                    return valDesc->name();
                }

                default:
                    ythrow yexception() << "type " << d->type_name() << " cannot be used as a field value";
            }

            return {};
        }

        double ReadFieldAsDouble(const NProtoBuf::Message& msg, const NProtoBuf::FieldDescriptor* d, const NProtoBuf::Reflection& r) {
            using namespace NProtoBuf;

            switch (d->type()) {
                case FieldDescriptor::TYPE_DOUBLE:
                    return r.GetDouble(msg, d);
                case FieldDescriptor::TYPE_BOOL:
                    return r.GetBool(msg, d) ? 1 : 0;
                case FieldDescriptor::TYPE_INT32:
                    return r.GetInt32(msg, d);
                case FieldDescriptor::TYPE_INT64:
                    return r.GetInt64(msg, d);
                case FieldDescriptor::TYPE_UINT32:
                    return r.GetUInt32(msg, d);
                case FieldDescriptor::TYPE_UINT64:
                    return r.GetUInt64(msg, d);
                case FieldDescriptor::TYPE_SINT32:
                    return r.GetInt32(msg, d);
                case FieldDescriptor::TYPE_SINT64:
                    return r.GetInt64(msg, d);
                case FieldDescriptor::TYPE_FIXED32:
                    return r.GetUInt32(msg, d);
                case FieldDescriptor::TYPE_FIXED64:
                    return r.GetUInt64(msg, d);
                case FieldDescriptor::TYPE_SFIXED32:
                    return r.GetInt32(msg, d);
                case FieldDescriptor::TYPE_SFIXED64:
                    return r.GetInt64(msg, d);
                case FieldDescriptor::TYPE_FLOAT:
                    return r.GetFloat(msg, d);
                case FieldDescriptor::TYPE_ENUM:
                    return r.GetEnumValue(msg, d);
                default:
                    ythrow yexception() << "type " << d->type_name() << " cannot be used as a field value";
            }

            return std::numeric_limits<double>::quiet_NaN();
        }

        double ReadRepeatedAsDouble(const NProtoBuf::Message& msg, const NProtoBuf::FieldDescriptor* d, const NProtoBuf::Reflection& r, size_t i) {
            using namespace NProtoBuf;

            switch (d->type()) {
                case FieldDescriptor::TYPE_DOUBLE:
                    return r.GetRepeatedDouble(msg, d, i);
                case FieldDescriptor::TYPE_BOOL:
                    return r.GetRepeatedBool(msg, d, i) ? 1 : 0;
                case FieldDescriptor::TYPE_INT32:
                    return r.GetRepeatedInt32(msg, d, i);
                case FieldDescriptor::TYPE_INT64:
                    return r.GetRepeatedInt64(msg, d, i);
                case FieldDescriptor::TYPE_UINT32:
                    return r.GetRepeatedUInt32(msg, d, i);
                case FieldDescriptor::TYPE_UINT64:
                    return r.GetRepeatedUInt64(msg, d, i);
                case FieldDescriptor::TYPE_SINT32:
                    return r.GetRepeatedInt32(msg, d, i);
                case FieldDescriptor::TYPE_SINT64:
                    return r.GetRepeatedInt64(msg, d, i);
                case FieldDescriptor::TYPE_FIXED32:
                    return r.GetRepeatedUInt32(msg, d, i);
                case FieldDescriptor::TYPE_FIXED64:
                    return r.GetRepeatedUInt64(msg, d, i);
                case FieldDescriptor::TYPE_SFIXED32:
                    return r.GetRepeatedInt32(msg, d, i);
                case FieldDescriptor::TYPE_SFIXED64:
                    return r.GetRepeatedInt64(msg, d, i);
                case FieldDescriptor::TYPE_FLOAT:
                    return r.GetRepeatedFloat(msg, d, i);
                case FieldDescriptor::TYPE_ENUM:
                    return r.GetRepeatedEnumValue(msg, d, i);
                default:
                    ythrow yexception() << "type " << d->type_name() << " cannot be used as a field value";
            }

            return std::numeric_limits<double>::quiet_NaN();
        }

        TString LabelFromField(const NProtoBuf::Message& msg, const TString& name) {
            const auto* fieldDesc = msg.GetDescriptor()->FindFieldByName(name);
            const auto* reflection = msg.GetReflection();
            Y_ENSURE(fieldDesc && reflection, "Unable to get meta for field " << name);

            auto s = ReadLabelValue(msg, fieldDesc, *reflection);
            std::replace(std::begin(s), s.vend(), ' ', '_');

            return s;
        }

        TMaybeMeta MaybeGetMeta(const NProtoBuf::FieldOptions& opts) {
            if (opts.HasExtension(NMonProto::Metric)) {
                return opts.GetExtension(NMonProto::Metric);
            }

            return Nothing();
        }

        class ILabelGetter: public TThrRefBase {
        public:
            enum class EType {
                Fixed = 1,
                Lazy = 2,
            };

            virtual TLabel Get(const NProtoBuf::Message&) = 0;
            virtual EType Type() const = 0;
        };

        class TFixedLabel: public ILabelGetter {
        public:
            explicit TFixedLabel(TLabel&& l)
                : Label_{std::move(l)}
            {
                TRACE("found fixed label " << l);
            }

            EType Type() const override {
                return EType::Fixed;
            }
            TLabel Get(const NProtoBuf::Message&) override {
                return Label_;
            }

        private:
            TLabel Label_;
        };

        using TFunction = std::function<TLabel(const NProtoBuf::Message&)>;

        class TLazyLabel: public ILabelGetter {
        public:
            TLazyLabel(TFunction&& fn)
                : Fn_{std::move(fn)}
            {
                TRACE("found lazy label");
            }

            EType Type() const override {
                return EType::Lazy;
            }
            TLabel Get(const NProtoBuf::Message& msg) override {
                return Fn_(msg);
            }

        private:
            TFunction Fn_;
        };

        class TDecoderContext {
        public:
            void Init(const NProtoBuf::Message* msg) {
                Message_ = msg;
                Y_ENSURE(Message_);
                Reflection_ = msg->GetReflection();
                Y_ENSURE(Reflection_);

                for (auto it = Labels_.begin(); it != Labels_.end(); ++it) {
                    if ((*it)->Type() == ILabelGetter::EType::Lazy) {
                        auto l = (*it)->Get(Message());
                        *it = ::MakeIntrusive<TFixedLabel>(std::move(l));
                    } else {
                        auto l = (*it)->Get(Message());
                    }
                }
            }

            void Clear() noexcept {
                Message_ = nullptr;
                Reflection_ = nullptr;
            }

            TDecoderContext CreateChildFromMeta(const NMonProto::TMetricMeta& metricMeta, const TString& name, i64 repeatedIdx = -1) {
                TDecoderContext child{*this};
                child.Clear();

                if (metricMeta.HasCustomPath()) {
                    if (const auto& nodePath = metricMeta.GetCustomPath()) {
                        child.AppendPath(nodePath);
                    }
                } else if (metricMeta.GetPath()) {
                    child.AppendPath(name);
                }

                if (metricMeta.HasKeys()) {
                    child.ParseKeys(metricMeta.GetKeys(), repeatedIdx);
                }

                return child;
            }

            TDecoderContext CreateChildFromRepeatedScalar(const NMonProto::TMetricMeta& metricMeta, i64 repeatedIdx = -1) {
                TDecoderContext child{*this};
                child.Clear();

                if (metricMeta.HasKeys()) {
                    child.ParseKeys(metricMeta.GetKeys(), repeatedIdx);
                }

                return child;
            }

            TDecoderContext CreateChildFromEls(const TString& name, const NMonProto::TExtraLabelMetrics& metrics, size_t idx, TMaybeMeta maybeMeta) {
                TDecoderContext child{*this};
                child.Clear();

                auto usePath = [&maybeMeta] {
                    return !maybeMeta->HasPath() || maybeMeta->GetPath();
                };

                if (!name.empty() && (!maybeMeta || usePath())) {
                    child.AppendPath(name);
                }

                child.Labels_.push_back(::MakeIntrusive<TLazyLabel>(
                    [ labelName = metrics.GetlabelName(), idx, &metrics ](const auto&) {
                        const auto& val = metrics.Getvalues(idx);
                        TString labelVal;
                        const auto uintLabel = val.GetlabelValueUint();

                        if (uintLabel) {
                            labelVal = ::ToString(uintLabel);
                        } else {
                            labelVal = val.GetlabelValue();
                        }

                        return TLabel{labelName, labelVal};
                    }));

                return child;
            }

            void ParseKeys(TStringBuf keys, i64 repeatedIdx = -1) {
                auto parts = StringSplitter(keys)
                                 .Split(' ')
                                 .SkipEmpty();

                for (auto part : parts) {
                    auto str = part.Token();

                    TStringBuf lhs, rhs;

                    const bool isDynamic = str.TrySplit(':', lhs, rhs);
                    const bool isIndexing = isDynamic && rhs == TStringBuf("#");

                    if (isIndexing) {
                        TRACE("parsed index labels");

                        // <label_name>:# means that we should use index of the repeated
                        // field as label value
                        Y_ENSURE(repeatedIdx != -1);
                        Labels_.push_back(::MakeIntrusive<TLazyLabel>([=](const auto&) {
                            return TLabel{lhs, ::ToString(repeatedIdx)};
                        }));
                    } else if (isDynamic) {
                        TRACE("parsed dynamic labels");

                        // <label_name>:<field_name> means that we need to take label value
                        // later from message's field
                        Labels_.push_back(::MakeIntrusive<TLazyLabel>([=](const auto& msg) {
                            return TLabel{lhs, LabelFromField(msg, TString{rhs})};
                        }));
                    } else if (str.TrySplit('=', lhs, rhs)) {
                        TRACE("parsed static labels");

                        // <label_name>=<label_value> stands for constant label
                        Labels_.push_back(::MakeIntrusive<TFixedLabel>(TLabel{lhs, rhs}));
                    } else {
                        ythrow yexception() << "Incorrect Keys format";
                    }
                }
            }

            void AppendPath(TStringBuf fieldName) {
                Path_ += '/';
                Path_ += fieldName;
            }

            const TString& Path() const {
                return Path_;
            }

            TLabels Labels() const {
                TLabels result;
                for (auto&& l : Labels_) {
                    result.Add(l->Get(Message()));
                }

                return result;
            }

            const NProtoBuf::Message& Message() const {
                Y_VERIFY_DEBUG(Message_);
                return *Message_;
            }

            const NProtoBuf::Reflection& Reflection() const {
                return *Reflection_;
            }

        private:
            const NProtoBuf::Message* Message_{nullptr};
            const NProtoBuf::Reflection* Reflection_{nullptr};

            TString Path_;
            TVector<TIntrusivePtr<ILabelGetter>> Labels_;
        };

        class TDecoder {
        public:
            TDecoder(IMetricConsumer* consumer, const NProtoBuf::Message& message, TInstant timestamp)
                : Consumer_{consumer}
                , Message_{message}
                , Timestamp_{timestamp}
            {
            }

            void Decode() const {
                Consumer_->OnStreamBegin();
                DecodeToStream();
                Consumer_->OnStreamEnd();
            }

            void DecodeToStream() const {
                DecodeImpl(Message_, {});
            }

        private:
            static const NMonProto::TExtraLabelMetrics& ExtractExtraMetrics(TDecoderContext& ctx, const NProtoBuf::FieldDescriptor& f) {
                const auto& parent = ctx.Message();
                const auto& reflection = ctx.Reflection();
                auto& subMessage = reflection.GetMessage(parent, &f);

                return dynamic_cast<const NMonProto::TExtraLabelMetrics&>(subMessage);
            }

            void DecodeImpl(const NProtoBuf::Message& msg, TDecoderContext ctx) const {
                std::vector<const NProtoBuf::FieldDescriptor*> fields;

                ctx.Init(&msg);

                ctx.Reflection().ListFields(msg, &fields);

                for (const auto* f : fields) {
                    Y_ENSURE(f);

                    const auto& opts = f->options();
                    const auto isMessage = f->type() == NProtoBuf::FieldDescriptor::TYPE_MESSAGE;
                    const auto isExtraLabelMetrics = isMessage && f->message_type()->full_name() == "NMonProto.TExtraLabelMetrics";
                    const auto maybeMeta = MaybeGetMeta(opts);

                    if (!(maybeMeta || isExtraLabelMetrics)) {
                        continue;
                    }

                    if (isExtraLabelMetrics) {
                        const auto& extra = ExtractExtraMetrics(ctx, *f);
                        RecurseExtraLabelMetrics(ctx, extra, f->name(), maybeMeta);
                    } else if (isMessage) {
                        RecurseMessage(ctx, *maybeMeta, *f);
                    } else if (f->is_repeated()) {
                        RecurseRepeatedScalar(ctx, *maybeMeta, *f);
                    } else if (maybeMeta->HasType()) {
                        const auto val = ReadFieldAsDouble(msg, f, ctx.Reflection());
                        const bool isRate = maybeMeta->GetType() == NMonProto::EMetricType::RATE;
                        WriteMetric(val, ctx, f->name(), isRate);
                    }
                }
            }

            void RecurseRepeatedScalar(TDecoderContext ctx, const NMonProto::TMetricMeta& meta, const NProtoBuf::FieldDescriptor& f) const {
                auto&& msg = ctx.Message();
                auto&& reflection = ctx.Reflection();
                const bool isRate = meta.GetType() == NMonProto::EMetricType::RATE;

                // this is a repeated scalar field, which makes metric only if it's indexing
                for (auto i = 0; i < reflection.FieldSize(msg, &f); ++i) {
                    auto subCtx = ctx.CreateChildFromRepeatedScalar(meta, i);
                    subCtx.Init(&msg);
                    auto val = ReadRepeatedAsDouble(msg, &f, reflection, i);
                    WriteMetric(val, subCtx, f.name(), isRate);
                }
            }

            void RecurseExtraLabelMetrics(TDecoderContext ctx, const NMonProto::TExtraLabelMetrics& msg, const TString& name, const TMaybeMeta& meta) const {
                auto i = 0;
                for (const auto& val : msg.Getvalues()) {
                    auto subCtx = ctx.CreateChildFromEls(name, msg, i++, meta);
                    subCtx.Init(&val);

                    const bool isRate = val.Hastype()
                             ? val.Gettype() == NMonProto::EMetricType::RATE
                             : meta->GetType() == NMonProto::EMetricType::RATE;

                    double metricVal{0};
                    if (isRate) {
                        metricVal = val.GetlongValue();
                    } else {
                        metricVal = val.GetdoubleValue();
                    }

                    WriteMetric(metricVal, subCtx, "", isRate);

                    for (const auto& child : val.Getchildren()) {
                        RecurseExtraLabelMetrics(subCtx, child, "", meta);
                    }
                }
            }

            void RecurseMessage(TDecoderContext ctx, const NMonProto::TMetricMeta& metricMeta, const NProtoBuf::FieldDescriptor& f) const {
                const auto& msg = ctx.Message();
                const auto& reflection = ctx.Reflection();

                if (f.is_repeated()) {
                    TRACE("recurse into repeated message " << f.name());
                    for (auto i = 0; i < reflection.FieldSize(msg, &f); ++i) {
                        auto& subMessage = reflection.GetRepeatedMessage(msg, &f, i);
                        DecodeImpl(subMessage, ctx.CreateChildFromMeta(metricMeta, f.name(), i));
                    }
                } else {
                    TRACE("recurse into message " << f.name());
                    auto& subMessage = reflection.GetMessage(msg, &f);
                    DecodeImpl(subMessage, ctx.CreateChildFromMeta(metricMeta, f.name()));
                }
            }

            inline void WriteValue(ui64 value) const {
                Consumer_->OnUint64(Timestamp_, value);
            }

            inline void WriteValue(double value) const {
                Consumer_->OnDouble(Timestamp_, value);
            }

            void WriteMetric(double value, const TDecoderContext& ctx, const TString& name, bool isRate) const {
                if (isRate) {
                    Consumer_->OnMetricBegin(EMetricType::RATE);
                    WriteValue(static_cast<ui64>(value));
                } else {
                    Consumer_->OnMetricBegin(EMetricType::GAUGE);
                    WriteValue(static_cast<double>(value));
                }

                Consumer_->OnLabelsBegin();

                for (const auto& label : ctx.Labels()) {
                    Consumer_->OnLabel(label.Name(), label.Value());
                }

                const auto fullPath = name.empty()
                                          ? ctx.Path()
                                          : ctx.Path() + '/' + name;

                if (fullPath) {
                    Consumer_->OnLabel("path", fullPath);
                }

                Consumer_->OnLabelsEnd();
                Consumer_->OnMetricEnd();
            }

        private:
            IMetricConsumer* Consumer_{nullptr};
            const NProtoBuf::Message& Message_;
            TInstant Timestamp_;
        };

    }

    void DecodeLegacyProto(const NProtoBuf::Message& data, IMetricConsumer* consumer, TInstant ts) {
        Y_ENSURE(consumer);
        TDecoder(consumer, data, ts).Decode();
    }

    void DecodeLegacyProtoToStream(const NProtoBuf::Message& data, IMetricConsumer* consumer, TInstant ts) {
        Y_ENSURE(consumer);
        TDecoder(consumer, data, ts).DecodeToStream();
    }
}
