#pragma once

#include "byte_writer.h"
#include "cescape.h"
#include "percent_scalar.h"
#include "stream_counter.h"
#include "symbols.h"
#include "varint.h"

#include <library/cpp/yson_pull/consumer.h>
#include <library/cpp/yson_pull/event.h>
#include <library/cpp/yson_pull/output.h>
#include <library/cpp/yson_pull/stream_type.h>
#include <library/cpp/yson_pull/writer.h>

#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <cmath>

namespace NYsonPull {
    namespace NDetail {
        class writer: public IConsumer {
            enum class state {
                maybe_key,
                maybe_value,
                value,
                value_noattr,
                before_begin,
                before_end,
                after_end,
            };

            byte_writer<stream_counter<false>> stream_;
            TVector<EEventType> stack_;
            bool need_item_separator_ = false;
            EStreamType mode_ = EStreamType::ListFragment;
            state state_ = state::before_begin;

        public:
            void OnBeginStream() override {
                update_state(EEventType::BeginStream);
            }

            void OnEndStream() override {
                update_state(EEventType::EndStream);
                stream_.flush_buffer();
            }

            void OnBeginList() override {
                begin_node();
                write(NSymbol::begin_list);
                update_state(EEventType::BeginList);
                begin_collection(collection_type::list);
            }

            void OnEndList() override {
                update_state(EEventType::EndList);
                end_collection(collection_type::list);
                write(NSymbol::end_list);
                end_node();
            }

            void OnBeginMap() override {
                begin_node();
                write(NSymbol::begin_map);
                update_state(EEventType::BeginMap);
                begin_collection(collection_type::map);
            }

            void OnEndMap() override {
                update_state(EEventType::EndMap);
                end_collection(collection_type::map);
                write(NSymbol::end_map);
                end_node();
            }

            void OnBeginAttributes() override {
                begin_node();
                write(NSymbol::begin_attributes);
                update_state(EEventType::BeginAttributes);
                begin_collection(collection_type::attributes);
            }

            void OnEndAttributes() override {
                update_state(EEventType::EndAttributes);
                end_collection(collection_type::attributes);
                write(NSymbol::end_attributes);
                // no end_node
            }

            void OnEntity() override {
                begin_node();
                update_state(EEventType::Scalar);
                write(NSymbol::entity);
                end_node();
            }

        protected:
            enum class collection_type {
                list,
                map,
                attributes,
            };

            writer(NYsonPull::NOutput::IStream& stream, EStreamType mode)
                : stream_(stream)
                , mode_{mode} {
            }

            bool need_item_separator() const {
                return need_item_separator_;
            }
            void need_item_separator(bool value) {
                need_item_separator_ = value;
            }

            size_t depth() const {
                Y_ASSERT(!stack_.empty());
                if (mode_ == EStreamType::Node) {
                    return stack_.size() - 1;
                } else {
                    return stack_.size() - 2;
                }
            }
            EStreamType mode() const {
                return mode_;
            }

            void write(ui8 c) {
                stream_.write(c);
            }

            void write(TStringBuf value) {
                write_raw(value.data(), value.size());
            }

            void write_raw(const void* ptr, size_t len) {
                stream_.write(static_cast<const ui8*>(ptr), len);
            }

            template <typename T>
            void write_varint(T value) {
                NVarInt::write(stream_, value);
            }

            void write_escaped_string(TStringBuf value) {
                write(NSymbol::quote);
                NCEscape::encode(stream_, value);
                write(NSymbol::quote);
            }

            void push(EEventType type) {
                stack_.push_back(type);
            }

            void pop(EEventType type) {
                if (stack_.empty()) {
                    fail("Unpaired events: empty event stack");
                }
                if (stack_.back() != type) {
                    fail("Unpaired events: expected ", type, ", got ", stack_.back());
                }
                stack_.pop_back();
            }

            void update_state(EEventType event) {
                switch (state_) {
                    case state::before_begin:
                        if (event != EEventType::BeginStream) {
                            fail("Expected begin_stream, got ", event);
                        }
                        begin_stream();
                        return;

                    case state::before_end:
                        if (event != EEventType::EndStream) {
                            fail("Expected end_stream, got ", event);
                        }
                        end_stream();
                        return;

                    case state::after_end:
                        fail("Attempted write past stream end");

                    case state::maybe_key:
                        if (event == EEventType::Key) {
                            state_ = state::value;
                            return;
                        }

                        switch (event) {
                            case EEventType::EndStream:
                                end_stream();
                                return;

                            case EEventType::EndMap:
                                pop(EEventType::BeginMap);
                                next_state();
                                return;

                            case EEventType::EndAttributes:
                                pop(EEventType::BeginAttributes);
                                state_ = state::value_noattr;
                                return;

                            default:
                                fail("Unexpected event ", event, " in maybe_key");
                        }
                        break;

                    case state::maybe_value:
                        switch (event) {
                            case EEventType::EndList:
                                pop(EEventType::BeginList);
                                next_state();
                                return;

                            case EEventType::EndStream:
                                end_stream();
                                return;

                            default:
                                break;
                        }
                        [[fallthrough]];
                    case state::value:
                        if (event == EEventType::BeginAttributes) {
                            push(EEventType::BeginAttributes);
                            next_state();
                            return;
                        }
                        [[fallthrough]];
                    case state::value_noattr:
                        switch (event) {
                            case EEventType::Scalar:
                                next_state();
                                return;

                            case EEventType::BeginList:
                                push(EEventType::BeginList);
                                next_state();
                                return;

                            case EEventType::BeginMap:
                                push(EEventType::BeginMap);
                                next_state();
                                return;

                            default:
                                fail("Unexpected event ", event, " (in value_*)");
                        }
                        break;
                }
            }

            void next_state() {
                Y_ASSERT(!stack_.empty());
                switch (stack_.back()) {
                    case EEventType::BeginMap:
                    case EEventType::BeginAttributes:
                        state_ = state::maybe_key;
                        break;

                    case EEventType::BeginList:
                        state_ = state::maybe_value;
                        break;

                    case EEventType::BeginStream:
                        state_ = state::before_end;
                        break;

                    default:
                        Y_UNREACHABLE();
                }
            }

            void begin_stream() {
                push(EEventType::BeginStream);
                switch (mode_) {
                    case EStreamType::ListFragment:
                        push(EEventType::BeginList);
                        state_ = state::maybe_value;
                        break;

                    case EStreamType::MapFragment:
                        push(EEventType::BeginMap);
                        state_ = state::maybe_key;
                        break;

                    case EStreamType::Node:
                        state_ = state::value;
                        break;
                }
            }

            void end_stream() {
                switch (mode_) {
                    case EStreamType::ListFragment:
                        pop(EEventType::BeginList);
                        break;

                    case EStreamType::MapFragment:
                        pop(EEventType::BeginMap);
                        break;

                    case EStreamType::Node:
                        break;
                }
                pop(EEventType::BeginStream);
                state_ = state::after_end;
            }

            virtual void begin_node() {
                if (need_item_separator_) {
                    write(NSymbol::item_separator);
                }
            }

            virtual void end_node() {
                need_item_separator_ = true;
            }

            virtual void begin_key() {
                begin_node();
            }

            virtual void end_key() {
                need_item_separator_ = false;
                write(NSymbol::key_value_separator);
            }

            virtual void begin_collection(collection_type type) {
                Y_UNUSED(type);
                need_item_separator_ = false;
            }

            virtual void end_collection(collection_type type) {
                need_item_separator_ = (type != collection_type::attributes);
            }

            template <typename... Args>
            ATTRIBUTE(noinline, cold)
            void fail[[noreturn]](const char* msg, Args&&... args) {
                auto formatted_message = format_string(
                    msg,
                    std::forward<Args>(args)...);
                throw NException::TBadOutput(
                    formatted_message,
                    stream_.counter().info());
            }
        };

        class TBinaryWriterImpl final: public writer {
        public:
            TBinaryWriterImpl(NYsonPull::NOutput::IStream& stream, EStreamType mode)
                : writer(stream, mode)
            {
            }

            void OnScalarBoolean(bool value) override {
                update_state(EEventType::Scalar);

                begin_node();
                write(value ? NSymbol::true_marker : NSymbol::false_marker);
                end_node();
            }

            void OnScalarInt64(i64 value) override {
                update_state(EEventType::Scalar);

                begin_node();
                write(NSymbol::int64_marker);
                write_varint(value);
                end_node();
            }

            void OnScalarUInt64(ui64 value) override {
                update_state(EEventType::Scalar);

                begin_node();
                write(NSymbol::uint64_marker);
                write_varint(value);
                end_node();
            }

            void OnScalarFloat64(double value) override {
                update_state(EEventType::Scalar);

                begin_node();
                write(NSymbol::double_marker);
                write_raw(&value, sizeof value);
                end_node();
            }

            void OnScalarString(TStringBuf value) override {
                update_state(EEventType::Scalar);

                begin_node();
                write(NSymbol::string_marker);
                write_varint(static_cast<i32>(value.size()));
                write_raw(value.data(), value.size());
                end_node();
            }

            void OnKey(TStringBuf name) override {
                update_state(EEventType::Key);

                begin_key();
                write(NSymbol::string_marker);
                write_varint(static_cast<i32>(name.size()));
                write_raw(name.data(), name.size());
                end_key();
            }
        };

        class TTextWriterImpl: public writer {
        public:
            TTextWriterImpl(NYsonPull::NOutput::IStream& stream, EStreamType mode)
                : writer(stream, mode)
            {
            }

            void OnScalarBoolean(bool value) override {
                update_state(EEventType::Scalar);

                begin_node();
                write(value ? percent_scalar::true_literal : percent_scalar::false_literal);
                end_node();
            }

            void OnScalarInt64(i64 value) override {
                update_state(EEventType::Scalar);

                char buf[32];
                auto len = ::snprintf(buf, sizeof(buf), "%" PRIi64, value);

                begin_node();
                write_raw(buf, len);
                end_node();
            }

            void OnScalarUInt64(ui64 value) override {
                update_state(EEventType::Scalar);

                char buf[32];
                auto len = ::snprintf(buf, sizeof(buf), "%" PRIu64, value);

                begin_node();
                write_raw(buf, len);
                write('u');
                end_node();
            }

            void OnScalarFloat64(double value) override {
                update_state(EEventType::Scalar);

                begin_node();

                if (std::isfinite(value)) {
                    char buf[32];
                    auto len = ::snprintf(buf, sizeof(buf), "%#.17lg", value);
                    write_raw(buf, len);
                } else if (std::isnan(value)) {
                    write(percent_scalar::nan_literal);
                } else if (value > 0) {
                    write(percent_scalar::positive_inf_literal);
                } else {
                    write(percent_scalar::negative_inf_literal);
                }

                end_node();
            }

            void OnScalarString(TStringBuf value) override {
                update_state(EEventType::Scalar);

                begin_node();
                write_escaped_string(value);
                end_node();
            }

            void OnKey(TStringBuf name) override {
                update_state(EEventType::Key);

                begin_key();
                write_escaped_string(name);
                end_key();
            }

        protected:
            void begin_node() override {
                if (need_item_separator()) {
                    write(NSymbol::item_separator);
                    write(' ');
                }
            }

            void end_node() override {
                if (mode() != EStreamType::Node && depth() == 0) {
                    write(NSymbol::item_separator);
                    write('\n');
                    need_item_separator(false);
                } else {
                    writer::end_node();
                }
            }

            void end_key() override {
                write(' ');
                writer::end_key();
                write(' ');
            }
        };

        class TPrettyWriterImpl final: public TTextWriterImpl {
            size_t indent_size_;

        public:
            TPrettyWriterImpl(
                NYsonPull::NOutput::IStream& stream,
                EStreamType mode,
                size_t indent_size)
                : TTextWriterImpl(stream, mode)
                , indent_size_{indent_size} {
            }

        protected:
            void begin_node() override {
                if (need_item_separator()) {
                    write(NSymbol::item_separator);
                    newline();
                }
            }

            void begin_collection(collection_type type) override {
                TTextWriterImpl::begin_collection(type);
                newline();
            }

            void end_collection(collection_type type) override {
                TTextWriterImpl::end_collection(type);
                newline();
            }

            void newline() {
                write('\n');
                indent(depth());
            }

            void indent(size_t count) {
                for (size_t i = 0; i < count * indent_size_; ++i) {
                    write(' ');
                }
            }
        };

        template <typename T, typename... Args>
        NYsonPull::TWriter make_writer(
            THolder<NYsonPull::NOutput::IStream> stream,
            Args&&... args) {
            auto impl = MakeHolder<T>(*stream, std::forward<Args>(args)...);
            return NYsonPull::TWriter(std::move(stream), std::move(impl));
        }
    }
}
