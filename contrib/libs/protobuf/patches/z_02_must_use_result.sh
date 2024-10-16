sed -e 's|bool Parse|PROTOBUF_MUST_USE_RESULT bool Parse|' \
    -e 's|bool Merge|PROTOBUF_MUST_USE_RESULT bool Merge|' \
    -e 's|bool Serialize|PROTOBUF_MUST_USE_RESULT bool Serialize|' \
    -e 's|bool Append|PROTOBUF_MUST_USE_RESULT bool Append|' \
    -i src/google/protobuf/message_lite.h
