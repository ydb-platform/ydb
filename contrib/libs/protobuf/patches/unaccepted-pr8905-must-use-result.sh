# Kudos for negative lookahead implementation in sed go to
# https://stackoverflow.com/questions/12176026/whats-wrong-with-my-lookahead-regex-in-gnu-sed
sed --regexp-extended \
    --expression='/MergeFromImpl/! s/bool (Parse|Merge|Serialize|Append)/[[nodiscard]] bool \1/g' \
    --in-place src/google/protobuf/message_lite.h
