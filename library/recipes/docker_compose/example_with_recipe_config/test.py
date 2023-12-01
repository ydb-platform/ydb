import os
import logging

import yatest.common


def test():
    os.makedirs("/tmp/output")
    with open("/tmp/output/out.txt", "w") as f:
        res = yatest.common.execute("/app/bin/hello")
        f.write("/bin/hello stdout: {}".format(res.std_out))
    logging.info("out: %s", res.std_out)
