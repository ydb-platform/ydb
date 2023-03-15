#!/usr/bin/env python
import sys
import os.path

if __name__ == '__main__':
    output = sys.argv[1]
    size_sum = 0
    for filename in sys.argv[2:]:
        if os.path.exists(filename):
            size_sum += os.path.getsize(filename)
        else:
            sys.stderr.write('write_file_size.py: {0}: No such file or directory\n'.format(filename))
            sys.exit(1)
    with open(output, 'w') as f:
        f.write(str(size_sum))
