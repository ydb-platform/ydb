#pragma once

// google/protobuf/messagext.h is an Arcadia-only patch on top of protobuf and
// has no counterpart in the vanilla runtime, so the few pieces of it this
// library needs are reimplemented here on public protobuf API instead.

#include <google/protobuf/io/zero_copy_stream_impl.h>

#include <util/stream/input.h>
#include <util/stream/output.h>

namespace google {
    namespace protobuf {
        class Message;
    } // namespace protobuf
} // namespace google

namespace NProtoBufUtil {
    class TErrorState {
    public:
        bool HasError() const {
            return HasError_;
        }

        void SetError() {
            HasError_ = true;
        }

    private:
        bool HasError_ = false;
    };

    class TInputStreamProxy: public google::protobuf::io::CopyingInputStream, public TErrorState {
    public:
        explicit TInputStreamProxy(IInputStream* slave)
            : Slave_(slave)
        {
        }

        int Read(void* buffer, int size) override;

    private:
        IInputStream* Slave_;
    };

    class TOutputStreamProxy: public google::protobuf::io::CopyingOutputStream, public TErrorState {
    public:
        explicit TOutputStreamProxy(IOutputStream* slave)
            : Slave_(slave)
        {
        }

        bool Write(const void* buffer, int size) override;

    private:
        IOutputStream* Slave_;
    };

    class TCopyingInputStreamAdaptor: public TInputStreamProxy, public google::protobuf::io::CopyingInputStreamAdaptor {
    public:
        explicit TCopyingInputStreamAdaptor(IInputStream* input)
            : TInputStreamProxy(input)
            , google::protobuf::io::CopyingInputStreamAdaptor(this)
        {
        }
    };

    class TCopyingOutputStreamAdaptor: public TOutputStreamProxy, public google::protobuf::io::CopyingOutputStreamAdaptor {
    public:
        explicit TCopyingOutputStreamAdaptor(IOutputStream* output)
            : TOutputStreamProxy(output)
            , google::protobuf::io::CopyingOutputStreamAdaptor(this)
        {
        }
    };

    bool ParseFromArcadiaStream(google::protobuf::Message& msg, IInputStream* input);
    bool SerializeToArcadiaStream(const google::protobuf::Message& msg, IOutputStream* output);
} // namespace NProtoBufUtil
