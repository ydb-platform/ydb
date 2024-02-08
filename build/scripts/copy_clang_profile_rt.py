import optparse
import os
import shutil

import process_command_files as pcf

# List is a temporary thing to ensure that nothing breaks before and after switching to newer clang
# Remove after DTCC-1902
CLANG_RT_VERSIONS = [14, 16]


def copy_clang_rt_profile(cmd, build_root, arch):
    profile_rt_lib = None
    resource_dir = None

    for arg in cmd:
        for version in CLANG_RT_VERSIONS:
            if arg.startswith(f'contrib/libs/clang{version}-rt/lib/profile/libclang_rt.profile'):
                profile_rt_lib = arg
                break
        if arg.startswith('-resource-dir='):
            resource_dir = arg[len('-resource-dir=') :]

    profile_rt_path = os.path.join(build_root, profile_rt_lib)
    profile_name = os.path.basename(profile_rt_path)

    dst_dir = os.path.join(build_root, resource_dir, 'lib/{}'.format(arch.lower()))
    os.makedirs(dst_dir, exist_ok=True)
    shutil.copy(profile_rt_path, os.path.join(dst_dir, profile_name))


def parse_args():
    parser = optparse.OptionParser()
    parser.disable_interspersed_args()
    parser.add_option('--build-root')
    parser.add_option('--arch')
    return parser.parse_args()


if __name__ == '__main__':
    opts, args = parse_args()
    args = pcf.skip_markers(args)
    copy_clang_rt_profile(args, opts.build_root, opts.arch)
