#!/usr/bin/env python3

import subprocess
import resource
import sys
import json

rusage_file = sys.argv[1]
kqprun = subprocess.Popen(['./kqprun'] + sys.argv[2:], stdin=None)
stdout, stderr = kqprun.communicate()

ru = resource.getrusage(resource.RUSAGE_CHILDREN)

with open(rusage_file, 'wt') as rf:
    res = {
        'utime': ru.ru_utime,
        'stime': ru.ru_stime,
        'maxrss': ru.ru_maxrss,
    }
    print(json.dumps(res), file=rf)

sys.exit(kqprun.returncode)
