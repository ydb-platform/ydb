cat << EOF > fix.py
import sys

def fix_line(l):
    l = l.replace('absl/', 'y_absl/')
    l = l.replace('absl::', 'y_absl::')
    l = l.replace('ABSL_', 'Y_ABSL_')
    l = l.replace('std::string', 'TProtoStringType')

    return l

print('\n'.join(fix_line(x) for x in sys.stdin.read().split('\n')).strip())
EOF

(
    find . -type f -name '*.cc'
    find . -type f -name '*.h'
) | while read l; do
    cat ${l} | python3 ./fix.py > _
    mv _ ${l}
done

rm fix.py
