import subprocess
import sys
import os
import re
import argparse
import json

CLANG_SA_CONFIG='static_analyzer.json'

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--testing-src", required=True)
    parser.add_argument("--clang-bin", required=True)
    parser.add_argument("--source-root", required=True)
    parser.add_argument("--config-file", required=True)
    parser.add_argument("--plugins-begin", dest='plugins', action='append', nargs='+', required=True)
    parser.add_argument("--plugins-end", action='store_true', required=True)
    return parser.parse_known_args()

def find_config(config_path):
    # For unifying config files names
    basename = os.path.basename(config_path)
    if basename != CLANG_SA_CONFIG:
        msg = "The config file should be called {}, but {} passed".format(CLANG_SA_CONFIG, basename)
        raise ValueError(msg)
    if not os.path.isfile(config_path):
        raise ValueError("Cant find config file {}".format(config_path))
    return config_path

def parse_config(config_file):
    conf = None
    try:
        with open(config_file, 'r') as afile:
            conf = json.load(afile)
    except:
        conf = None
    return conf

def should_analyze(filename, conf):
    include_files = conf.get('include_files')
    exclude_files = conf.get('exclude_files')

    if not include_files:
        return False

    include = re.match(include_files, filename)
    exclude = re.match(exclude_files, filename) if exclude_files else False

    return include and not exclude

def load_plugins(conf, plugins):
    load_cmds = []
    for plugin in filter(lambda path: os.path.isfile(path), plugins):
        load_cmds.extend(["-Xclang", "-load", "-Xclang", plugin])
    return load_cmds

def main():
    args, clang_cmd = parse_args()

    # Try to find config file and parse them
    config_file = find_config(args.config_file)
    conf = parse_config(config_file)

    # Ensure we can read config
    if not conf:
        raise ValueError(f"Cant parse config file, check its syntax: {config_file}")

    # Ensure we have at least one check
    if ('checks' not in conf) or (not conf['checks']):
        raise ValueError("There are no checks in the config file")

    # Ensure that file match regex
    if not should_analyze(args.testing_src, conf):
        return 0

    # Prepare args
    analyzer_opts = [
        '-Wno-unused-command-line-argument',
        '--analyze',
        '--analyzer-outputtext',
        '--analyzer-no-default-checks'
    ]
    analyzer_opts.extend(['-Xanalyzer', '-analyzer-werror'])
    analyzer_opts.extend(['-Xanalyzer', '-analyzer-checker=' + ','.join(conf['checks'])])

    # Load additional plugins
    analyzer_opts.extend(load_plugins(conf, args.plugins[0]))

    run_cmd = [args.clang_bin, args.testing_src] + clang_cmd + analyzer_opts
    p = subprocess.run(run_cmd)

    return p.returncode

if __name__ == '__main__':
    ret_code = 0
    try:
        ret_code = main()
    except Exception as e:
        print("\n[Error]: " + str(e), file=sys.stderr)
        ret_code = 1
    exit(ret_code)

