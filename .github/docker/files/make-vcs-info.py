#!/usr/bin/env python3
import argparse
import json
import os
import subprocess


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('path')
    parser.add_argument('out', type=argparse.FileType('w'))
    args = parser.parse_args()

    os.chdir(os.path.expanduser(args.path))
    git_show_cmd = 'git show -s --format=\'{"ref": "%D", "sha": "%H","author": "%an <%ae>","date":"%ad","commit_message": "%s"}\' HEAD'
    output = subprocess.getoutput(git_show_cmd)
    print(f'git output: {output}')
    git_info = json.loads(output)

    refname = os.environ['GIT_REF']
    # refname = git_info['ref']
    commit = git_info['sha']
    author = git_info['author']
    summary = git_info['commit_message']

    scm_data = f'''Git info:
    refname: {refname}
    Commit: {commit}
    Author: {author}
    Summary: {summary}    
'''

    vcs_info = {
        'BRANCH': refname,
        'SCM_DATA': scm_data,
        'PROGRAM_VERSION': scm_data
    }

    json.dump(vcs_info, args.out, indent=4)


if __name__ == '__main__':
    main()
