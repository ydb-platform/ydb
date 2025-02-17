#!/usr/bin/env python

try:
    from shlex import quote
except ImportError:
    from pipes import quote

bashcode = r'''
# Run something, muting output or redirecting it to the debug stream
# depending on the value of _ARC_DEBUG.
# If ARGCOMPLETE_USE_TEMPFILES is set, use tempfiles for IPC.
__python_argcomplete_run() {
    if [[ -z "${ARGCOMPLETE_USE_TEMPFILES-}" ]]; then
        __python_argcomplete_run_inner "$@"
        return
    fi
    local tmpfile="$(mktemp)"
    _ARGCOMPLETE_STDOUT_FILENAME="$tmpfile" __python_argcomplete_run_inner "$@"
    local code=$?
    cat "$tmpfile"
    rm "$tmpfile"
    return $code
}

__python_argcomplete_run_inner() {
    if [[ -z "${_ARC_DEBUG-}" ]]; then
        "$@" 8>&1 9>&2 1>/dev/null 2>&1
    else
        "$@" 8>&1 9>&2 1>&9 2>&1
    fi
}

_python_argcomplete%(function_suffix)s() {
    local IFS=$'\013'
    local SUPPRESS_SPACE=0
    if compopt +o nospace 2> /dev/null; then
        SUPPRESS_SPACE=1
    fi
    COMPREPLY=( $(IFS="$IFS" \
                  COMP_LINE="$COMP_LINE" \
                  COMP_POINT="$COMP_POINT" \
                  COMP_TYPE="$COMP_TYPE" \
                  _ARGCOMPLETE_COMP_WORDBREAKS="$COMP_WORDBREAKS" \
                  _ARGCOMPLETE=1 \
                  _ARGCOMPLETE_SUPPRESS_SPACE=$SUPPRESS_SPACE \
                  __python_argcomplete_run "%(argcomplete_script)s") )
    if [[ $? != 0 ]]; then
        unset COMPREPLY
    elif [[ $SUPPRESS_SPACE == 1 ]] && [[ "${COMPREPLY-}" =~ [=/:]$ ]]; then
        compopt -o nospace
    fi
}
complete %(complete_opts)s -F _python_argcomplete%(function_suffix)s %(executables)s
'''

tcshcode = '''\
complete "%(executable)s" 'p@*@`python-argcomplete-tcsh "%(argcomplete_script)s"`@' ;
'''

fishcode = r'''
function __fish_%(executable)s_complete
    set -x _ARGCOMPLETE 1
    set -x _ARGCOMPLETE_DFS \t
    set -x _ARGCOMPLETE_IFS \n
    set -x _ARGCOMPLETE_SUPPRESS_SPACE 1
    set -x _ARGCOMPLETE_SHELL fish
    set -x COMP_LINE (commandline -p)
    set -x COMP_POINT (string length (commandline -cp))
    set -x COMP_TYPE
    if set -q _ARC_DEBUG
        %(argcomplete_script)s 8>&1 9>&2 1>&9 2>&1
    else
        %(argcomplete_script)s 8>&1 9>&2 1>/dev/null 2>&1
    end
end
complete -c %(executable)s -f -a '(__fish_%(executable)s_complete)'
'''

shell_codes = {'bash': bashcode, 'tcsh': tcshcode, 'fish': fishcode}


def shellcode(executables, use_defaults=True, shell='bash', complete_arguments=None, argcomplete_script=None):
    '''
    Provide the shell code required to register a python executable for use with the argcomplete module.

    :param list(str) executables: Executables to be completed (when invoked exactly with this name)
    :param bool use_defaults: Whether to fallback to readline's default completion when no matches are generated.
    :param str shell: Name of the shell to output code for (bash or tcsh)
    :param complete_arguments: Arguments to call complete with
    :type complete_arguments: list(str) or None
    :param argcomplete_script: Script to call complete with, if not the executable to complete.
        If supplied, will be used to complete *all* passed executables.
    :type argcomplete_script: str or None
    '''

    if complete_arguments is None:
        complete_options = '-o nospace -o default -o bashdefault' if use_defaults else '-o nospace -o bashdefault'
    else:
        complete_options = " ".join(complete_arguments)

    if shell == 'bash':
        quoted_executables = [quote(i) for i in executables]
        executables_list = " ".join(quoted_executables)
        script = argcomplete_script
        if script:
            function_suffix = '_' + script
        else:
            script = '$1'
            function_suffix = ''
        code = bashcode % dict(complete_opts=complete_options, executables=executables_list,
                               argcomplete_script=script, function_suffix=function_suffix)
    else:
        code = ""
        for executable in executables:
            script = argcomplete_script
            # If no script was specified, default to the executable being completed.
            if not script:
                script = executable
            code += shell_codes.get(shell, '') % dict(executable=executable, argcomplete_script=script)

    return code
