cat << EOF > fix.py
import sys

def fix_line(l):
    l = l.replace('~basic_string()', '~TBasicString()')

    if 'ByteCount' in l and 'int64_t' in l and 'const' in l:
        return l

    if 'GetSupportedFeatures' in l and 'const' in l and 'uint64_t' in l:
        return l

    l = l.replace('std::uint64_t', 'arc_ui64').replace('std::int64_t', 'arc_i64')
    l = l.replace('std::uint32_t', 'arc_ui32').replace('std::int32_t', 'arc_i32')
    l = l.replace('uint64_t', 'arc_ui64').replace('int64_t', 'arc_i64')
    l = l.replace('uint32_t', 'arc_ui32').replace('int32_t', 'arc_i32')

    l = l.replace('"third_party/absl/', '"y_absl/')
    l = l.replace('"absl/', '"y_absl/')
    l = l.replace('absl::', 'y_absl::')
    l = l.replace('ABSL_', 'Y_ABSL_')
    l = l.replace('ts_unchecked_read', 'y_ts_unchecked_read')
    l = l.replace('TS_UNCHECKED_READ', 'Y_TS_UNCHECKED_READ')
    l = l.replace('GOOGLE_DCHECK(', 'Y_ABSL_DCHECK(')
    l = l.replace('GOOGLE_DCHECK_LE(', 'Y_ABSL_DCHECK_LE(')

    if 'Y_ABSL_DEPRECATED' in l: l = ''

    return l

print('\n'.join(fix_line(x) for x in sys.stdin.read().split('\n')).strip())
EOF

(
    find . -type f -name '*.cc'
    find . -type f -name '*.h'
    find . -type f -name '*.inc'
) | while read l; do
    cat ${l} | python3 ./fix.py > _
    mv _ ${l}
done

rm fix.py
