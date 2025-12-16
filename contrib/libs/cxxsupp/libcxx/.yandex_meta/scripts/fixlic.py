import sys

def flt():
    skip = False

    for l in sys.stdin.read().split('\n'):
        if skip:
            if '====' in l:
                skip = False

        if not skip:
            if '====' in l and 'include/any' in l:
                skip = True
            else:
                yield l

print('\n'.join(flt()).strip() + '\n')
