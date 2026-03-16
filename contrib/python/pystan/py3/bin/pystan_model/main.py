import argparse
import os.path
import re

from library.python.resource import resfs_read


def main():
    parser = argparse.ArgumentParser()
    arg = parser.add_argument
    arg('output', help='output pyx model path')
    arg('-n', metavar='name', help='model name (default: {output} basename)')
    arg('-i', metavar='path', help='stan cpp model include path (default: {name}.hpp)')
    args = parser.parse_args()

    name = args.n or os.path.splitext(os.path.basename(args.output))[0]
    include = args.i or name + '.hpp'

    s = resfs_read('contrib/python/pystan/py3/pystan/stanfit4model.pyx').decode('utf-8')
    s = re.sub(r'(?s)\n\n#.*? THIS IS A TEMPLATE.*?#-+', '', s, count=1)
    s = s.replace('$model_cppname.hpp', include)
    s = s.replace('${model_cppname}', name)
    s = s.replace('$model_cppname', name)

    with open(args.output, 'w') as f:
        f.write(s)
