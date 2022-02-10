#pragma once

#include "consumer.h"
#include "output.h"
#include "scalar.h"
#include "stream_type.h"

#include <memory>

namespace NYsonPull {
    //! \brief YSON writer facade class
    //!
    //! Owns a YSON consumer and a corresponding output stream.
    //! Methods invoke corresponding \p NYsonPull::IConsumer methods and can be chained.
    class TWriter {
        THolder<NOutput::IStream> Stream_;
        THolder<IConsumer> Impl_;

    public:
        TWriter(
            THolder<NOutput::IStream> stream,
            THolder<IConsumer> impl)
            : Stream_{std::move(stream)}
            , Impl_{std::move(impl)} {
        }

        //! \brief Get a reference to underlying consumer.
        //!
        //! Useful with \p NYsonPull::bridge
        IConsumer& GetConsumer() {
            return *Impl_;
        }

        TWriter& BeginStream() {
            Impl_->OnBeginStream();
            return *this;
        }
        TWriter& EndStream() {
            Impl_->OnEndStream();
            return *this;
        }

        TWriter& BeginList() {
            Impl_->OnBeginList();
            return *this;
        }
        TWriter& EndList() {
            Impl_->OnEndList();
            return *this;
        }

        TWriter& BeginMap() {
            Impl_->OnBeginMap();
            return *this;
        }
        TWriter& EndMap() {
            Impl_->OnEndMap();
            return *this;
        }

        TWriter& BeginAttributes() {
            Impl_->OnBeginAttributes();
            return *this;
        }
        TWriter& EndAttributes() {
            Impl_->OnEndAttributes();
            return *this;
        }

        TWriter& Key(TStringBuf name) {
            Impl_->OnKey(name);
            return *this;
        }

        TWriter& Entity() {
            Impl_->OnEntity();
            return *this;
        }
        TWriter& Boolean(bool value) {
            Impl_->OnScalarBoolean(value);
            return *this;
        }
        TWriter& Int64(i64 value) {
            Impl_->OnScalarInt64(value);
            return *this;
        }
        TWriter& UInt64(ui64 value) {
            Impl_->OnScalarUInt64(value);
            return *this;
        }
        TWriter& Float64(double value) {
            Impl_->OnScalarFloat64(value);
            return *this;
        }
        TWriter& String(TStringBuf value) {
            Impl_->OnScalarString(value);
            return *this;
        }

        TWriter& Scalar(const TScalar& value) {
            Impl_->OnScalar(value);
            return *this;
        }
        TWriter& Event(const TEvent& value) {
            Impl_->OnEvent(value);
            return *this;
        }
    };

    //! \brief Construct a writer for binary YSON format.
    TWriter MakeBinaryWriter(
        THolder<NOutput::IStream> stream,
        EStreamType mode);

    //! \brief Construct a writer for text YSON format.
    TWriter MakeTextWriter(
        THolder<NOutput::IStream> stream,
        EStreamType mode);

    //! \brief Construct a writer for pretty text YSON format.
    TWriter MakePrettyTextWriter(
        THolder<NOutput::IStream> stream,
        EStreamType mode,
        size_t indent_size = 4);

}
