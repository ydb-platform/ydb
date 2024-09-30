import sys
import os
import re
import subprocess
import platform


ERROR_PRONE_FLAGS = [
    '-Xep:FunctionalInterfaceMethodChanged:WARN',
    '-Xep:ReturnValueIgnored:WARN',
]

JAVA10_EXPORTS = [
    '--add-exports=jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED',
    '--add-exports=jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED',
    '--add-exports=jdk.compiler/com.sun.tools.javac.tree=ALL-UNNAMED',
    '--add-exports=jdk.compiler/com.sun.tools.javac.main=ALL-UNNAMED',
    '--add-exports=jdk.compiler/com.sun.tools.javac.code=ALL-UNNAMED',
    '--add-exports=jdk.compiler/com.sun.tools.javac.processing=ALL-UNNAMED',
    '--add-exports=jdk.compiler/com.sun.tools.javac.parser=ALL-UNNAMED',
    '--add-exports=jdk.compiler/com.sun.tools.javac.comp=ALL-UNNAMED',
]


def get_classpath(cmd):
    for i, part in enumerate(cmd):
        if part == '-classpath':
            i += 1
            if i < len(cmd):
                return cmd[i]
            else:
                return None
    return None


def parse_args(argv):
    parsed = []
    for i in range(len(argv)):
        if not argv[i].startswith('-'):
            parsed.append(argv[i])
            if len(parsed) >= 3:
                break
    return parsed + [argv[i + 1 :]]


def fix_cmd_line(error_prone_tool, cmd):
    if not error_prone_tool:
        return cmd
    for f in cmd:
        if f.startswith('-Xep'):
            ERROR_PRONE_FLAGS.append(f)
    for f in ERROR_PRONE_FLAGS:
        if f in cmd:
            cmd.remove(f)
    if '-processor' in cmd:
        classpath = get_classpath(cmd)
        if classpath:
            error_prone_tool = error_prone_tool + os.pathsep + classpath
    return (
        cmd
        + JAVA10_EXPORTS
        + ['-processorpath', error_prone_tool, '-XDcompilePolicy=byfile']
        + [(' '.join(['-Xplugin:ErrorProne'] + ERROR_PRONE_FLAGS))]
    )


# NOTE: legacy, only for "devtools/ya/jbuild"
def just_do_it(argv):
    java, javac, error_prone_tool, javac_cmd = parse_args(argv)
    for f in javac_cmd:
        if f.startswith('-Xep'):
            ERROR_PRONE_FLAGS.append(f)
    for f in ERROR_PRONE_FLAGS:
        if f in javac_cmd:
            javac_cmd.remove(f)
    if '-processor' in javac_cmd:
        classpath = get_classpath(javac_cmd)
        if classpath:
            error_prone_tool = error_prone_tool + os.pathsep + classpath
    cmd = (
        [javac]
        + JAVA10_EXPORTS
        + ['-processorpath', error_prone_tool, '-XDcompilePolicy=byfile']
        + [(' '.join(['-Xplugin:ErrorProne'] + ERROR_PRONE_FLAGS))]
        + javac_cmd
    )
    if platform.system() == 'Windows':
        sys.exit(subprocess.Popen(cmd).wait())
    else:
        os.execv(cmd[0], cmd)


if __name__ == '__main__':
    just_do_it(sys.argv[1:])
