import sys

from subprocess import Popen, PIPE

command = ['python3', '/home/mfilitov/ydbwork/ydb/ydb/library/benchmarks/ydb_testing/uploader.py']
process = Popen(command, stdout=PIPE, stderr=PIPE)
output, err = process.communicate()
print(output, file=sys.stderr)
print(err, file=sys.stderr)
