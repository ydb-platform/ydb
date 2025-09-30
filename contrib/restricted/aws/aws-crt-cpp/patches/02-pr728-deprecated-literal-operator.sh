set -xue
sed -e 's|operator"" |operator""|g' include/aws/crt/StringView.h -i
