#include "stream_adaptors.h"

#include <google/protobuf/message.h>

#include <util/generic/yexception.h>

namespace NProtoBufUtil {
    int TInputStreamProxy::Read(void* buffer, int size) {
        try {
            return static_cast<int>(Slave_->Read(buffer, static_cast<size_t>(size)));
        } catch (...) {
        }
        TErrorState::SetError();
        return -1;
    }

    bool TOutputStreamProxy::Write(const void* buffer, int size) {
        try {
            Slave_->Write(buffer, static_cast<size_t>(size));
            return true;
        } catch (...) {
        }
        TErrorState::SetError();
        return false;
    }

    bool ParseFromArcadiaStream(google::protobuf::Message& msg, IInputStream* input) {
        TInputStreamProxy proxy(input);
        bool res = false;
        {
            google::protobuf::io::CopyingInputStreamAdaptor stream(&proxy);
            res = msg.ParseFromZeroCopyStream(&stream);
        }
        return res && !proxy.HasError();
    }

    bool SerializeToArcadiaStream(const google::protobuf::Message& msg, IOutputStream* output) {
        TOutputStreamProxy proxy(output);
        bool res = false;
        {
            google::protobuf::io::CopyingOutputStreamAdaptor stream(&proxy);
            res = msg.SerializeToZeroCopyStream(&stream);
        }
        return res && !proxy.HasError();
    }
} // namespace NProtoBufUtil
