set -xue
sed -e 's|operator"" |operator""|g' onnx/common/interned_strings.h -i
