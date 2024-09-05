import json
import os
import sys

SHUTDOWN_SIGNAL = 'SIGUSR1'


class SignalInterruptionError(Exception):
    pass


def on_shutdown(s, f):
    raise SignalInterruptionError()


def mkdir_p(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


def _resolve_tmpdir_to_ram_drive_path(args):
    java_io_tmpdir_arg = '-Djava.io.tmpdir='
    for i, arg in enumerate(args):
        if arg.startswith(java_io_tmpdir_arg) and arg.split('=')[-1] == "${YA_TEST_JAVA_TMP_DIR}":
            try:
                with open(os.environ['YA_TEST_CONTEXT_FILE']) as afile:
                    context = json.load(afile)
                ram_tmpdir = context['runtime']['ram_drive_path']
            except Exception as e:
                ram_tmpdir = os.path.join(os.getcwd(), 'tests_tmp_dir')
                msg = "Warning: temp dir on ram drive was requested but ram drive path couldn't be obtained "
                msg += 'from context file due to error {!r}. '.format(e)
                msg += 'Temp dir in cwd will be used instead: {}\n'.format(ram_tmpdir)
                sys.stderr.write(msg)
                mkdir_p(ram_tmpdir)
            args[i] = java_io_tmpdir_arg + ram_tmpdir
            return


def main():
    args = sys.argv[1:]

    def execve():
        os.execve(args[0], args, os.environ)

    jar_binary = args[args.index('--jar-binary') + 1]
    java_bin_dir = os.path.dirname(jar_binary)
    jstack_binary = os.path.join(java_bin_dir, 'jstack.exe' if sys.platform == 'win32' else 'jstack')

    _resolve_tmpdir_to_ram_drive_path(args)

    if not os.path.exists(jstack_binary):
        sys.stderr.write("jstack is missing: {}\n".format(jstack_binary))
        execve()

    import signal

    signum = getattr(signal, SHUTDOWN_SIGNAL, None)

    if signum is None:
        execve()

    import subprocess

    proc = subprocess.Popen(args)
    signal.signal(signum, on_shutdown)
    timeout = False

    try:
        proc.wait()
    except SignalInterruptionError:
        sys.stderr.write("\nGot {} signal: going to shutdown junit\n".format(signum))
        # Dump stack traces
        subprocess.call([jstack_binary, str(proc.pid)], stdout=sys.stderr)
        # Kill junit - for more info see DEVTOOLS-7636
        os.kill(proc.pid, signal.SIGKILL)
        proc.wait()
        timeout = True

    if proc.returncode:
        sys.stderr.write('java exit code: {}\n'.format(proc.returncode))
        if timeout:
            # In case of timeout return specific exit code
            # https://a.yandex-team.ru/arc/trunk/arcadia/devtools/ya/test/const/__init__.py?rev=r8578188#L301
            proc.returncode = 10
            sys.stderr.write('java exit code changed to {}\n'.format(proc.returncode))

    return proc.returncode


if __name__ == '__main__':
    exit(main())
