import os
import sys
import json
import base64
import subprocess


if __name__ == '__main__':
    p = sys.argv.index('--')
    ctx = base64.b64decode(sys.argv[p + 1].encode()).decode()
    kv = {}

    for x in sys.argv[1:p]:
        k, v = x.split('=')
        ctx = ctx.replace(f'$({k})', v)
        kv[k] = v

    cmd = json.loads(ctx)

    args = cmd['cmd_args']
    cwd = cmd.get('cwd', kv['B'])

    env = dict(**os.environ)
    env['ARCADIA_ROOT_DISTBUILD'] = kv['S']
    env.update(cmd['env'])

    out = subprocess.check_output(args, env=env, cwd=cwd)

    if stdout := cmd.get('stdout'):
        with open(stdout, 'wb') as f:
            f.write(out)
