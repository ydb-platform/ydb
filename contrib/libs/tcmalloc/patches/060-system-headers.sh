set -eux
# DTCC-1856
find . -type f -name '*.h' | while read l; do
    sed -i '1s/^/#pragma clang system_header\n/' ${l}
done