import argparse
import json
import os
import re
import subprocess
from functools import lru_cache

EXTERNAL_PREFIXES = frozenset(
    [
        "boost/",
        "Eigen/",
        "unsupported/Eigen/",
        "tensorflow/",
        "third_party/tensorflow/",
        "contrib/",
        "opencv/",
        "opencv2/",
        "range/",
    ]
)

INCLUDE_PATTERN = re.compile(r'#\s*include\s*[<"]([^">]+)[">]')
COMMENT_PATTERN = re.compile(r'//.*')
FILE_PATTERN = re.compile(r'^([^\s]+) should (add|remove) these lines:$', re.MULTILINE)
ERROR_PATTERNS = frozenset(['error:', 'failed'])

SECTION_MARKERS = (
    (' should add these lines:', 'add'),
    (' should remove these lines:', 'remove'),
    ('The full include-list for ', 'full'),
)

# Cache for facade base prefixes
FACADE_BASES = frozenset(pfx.rstrip('/') for pfx in EXTERNAL_PREFIXES)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--testing-src", required=True)
    parser.add_argument("--iwyu-bin", required=True)
    parser.add_argument("--iwyu-json", required=True)
    parser.add_argument("--source-root", required=True)
    parser.add_argument("--build-root", required=True)
    parser.add_argument("--mapping_file", required=False, default="")
    parser.add_argument("--default-mapping-file", required=False, default="")
    parser.add_argument("--verbose", required=False, default="")
    return parser.parse_known_args()


def generate_outputs(output_json):
    """Create empty .o and .json"""
    output_obj = os.path.splitext(output_json)[0] + ".o"
    with open(output_obj, "w"):
        pass
    with open(output_json, "w"):
        pass


def filter_cmd(cmd):
    """Generator to filter command arguments after retry_cc.py."""
    skip = True
    for x in cmd:
        if not skip:
            yield x
        elif '/retry_cc.py' in x:
            skip = False


def filter_linker_flags(cmd):
    """Filter out unwanted linker flags and tool paths."""

    def should_filter(arg):
        if '/python' in arg and (arg.endswith('/python') or '/python3' in arg):
            return True
        if '/clang' in arg:
            if arg.endswith('/clang++'):
                return True
            elif arg.endswith('/clang'):
                return True
        if '/.ya/tools/' in arg:
            return True
        return False

    return [arg for arg in cmd if not should_filter(arg)]


def is_generated(testing_src, build_root):
    """Check if source file is generated."""
    return testing_src.startswith(build_root)


@lru_cache(maxsize=1024)
def parse_include_line(line):
    """Parse include line and cache results for better performance."""
    line = line.lstrip('- ').strip()
    line = COMMENT_PATTERN.sub('', line)
    match = INCLUDE_PATTERN.match(line)
    return match.group(1) if match else None


@lru_cache(maxsize=512)
def get_matching_key(path):
    """Get matching key for path with caching."""
    if not path:
        return None

    # Check external prefixes
    for pfx in EXTERNAL_PREFIXES:
        if path.startswith(pfx):
            return pfx.rstrip('/')

    # Check stdlib (no '/' in path)
    if '/' not in path:
        return None

    return path


def parse_iwyu_sections(iwyu_output, target_file=None):
    """Parse IWYU output into sections"""
    lines = iwyu_output.split('\n')
    sections = {}
    current_key = None
    current_file = None

    for line in lines:
        section_found = False
        for marker, key in SECTION_MARKERS:
            if marker in line:
                if key == 'full':
                    match = line.split(marker)
                    if len(match) > 1:
                        current_file = match[1].rstrip(":")
                        if not target_file or current_file == target_file:
                            current_key = key
                            sections[current_key] = []
                        else:
                            current_key = None
                else:
                    current_file = line.split(marker)[0]
                    if not target_file or current_file == target_file:
                        current_key = key
                        sections[current_key] = []
                    else:
                        current_key = None
                section_found = True
                break

        if not section_found and current_key is not None and (not target_file or current_file == target_file):
            sections[current_key].append(line)

    return sections


def create_include_maps(sections):
    def create_map(section_lines):
        result = {}
        for line in section_lines:
            inc = parse_include_line(line)
            key = get_matching_key(inc)
            if key:
                result.setdefault(key, []).append((line, inc))
        return result

    return (create_map(sections.get('add', [])), create_map(sections.get('remove', [])))


def filter_facade_to_private_replacements_for_file(iwyu_output, file_name):
    """Filter facade-to-private replacements for a specific file."""
    sections = parse_iwyu_sections(iwyu_output, file_name)

    if not sections:
        return ""

    add_map, remove_map = create_include_maps(sections)

    add_keys = set(add_map.keys())
    remove_keys = set(remove_map.keys())

    both_facade_keys = add_keys & remove_keys & FACADE_BASES
    only_add_facade_keys = (add_keys - remove_keys) & FACADE_BASES

    removed_from_add = set()
    for key in both_facade_keys | only_add_facade_keys:
        for _, inc in add_map.get(key, []):
            removed_from_add.add(inc)

    def should_keep_line(line, keys_to_skip):
        inc = parse_include_line(line)
        key = get_matching_key(inc)
        return key not in keys_to_skip

    filtered_add = [
        line for line in sections.get('add', []) if should_keep_line(line, both_facade_keys | only_add_facade_keys)
    ]

    filtered_remove = [line for line in sections.get('remove', []) if should_keep_line(line, both_facade_keys)]

    full_lines = sections.get('full', [])
    content_lines = []

    for line in full_lines:
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith('(') and 'has correct #includes/fwd-decls' in stripped:
            # Skip lines with "has correct #includes/fwd-decls"
            continue
        elif parse_include_line(line) not in removed_from_add:
            content_lines.append(line)

    added_to_full = set()
    for key in both_facade_keys:
        for line, _ in remove_map.get(key, []):
            clean = line.lstrip('-').strip()
            clean = COMMENT_PATTERN.sub('', clean).strip()
            if clean and clean not in added_to_full and clean not in content_lines:
                content_lines.append(clean)
                added_to_full.add(clean)
    result_parts = []
    if filtered_add:
        result_parts.extend(
            [f"{file_name} should add these lines:", *[line for line in filtered_add if line.strip()], ""]
        )
    if filtered_remove:
        result_parts.extend(
            [f"{file_name} should remove these lines:", *[line for line in filtered_remove if line.strip()], ""]
        )
    if content_lines:
        result_parts.append(f"The full include-list for {file_name}:")
        result_parts.extend([line for line in content_lines if line.strip()])
        result_parts.append("")

    return '\n'.join(result_parts).rstrip()


def filter_facade_to_private_replacements(iwyu_output):
    """Parse IWYU output for multiple files and apply facade filtering to each."""
    file_matches = list(FILE_PATTERN.finditer(iwyu_output))

    if not file_matches:
        return iwyu_output

    files = []
    seen = set()
    for match in file_matches:
        file_name = match.group(1)
        if file_name not in seen:
            files.append(file_name)
            seen.add(file_name)

    # Process each file
    processed_sections = []
    for file_name in files:
        filtered_output = filter_facade_to_private_replacements_for_file(iwyu_output, file_name)
        if filtered_output.strip():
            processed_sections.append(filtered_output)

    return '\n---\n\n'.join(processed_sections)


def iwyu_changes_empty_simple(output, file_name):
    """Check if IWYU changes are empty using simple parsing."""
    sections = parse_iwyu_sections(output)

    add_empty = all(not line.strip() for line in sections.get('add', []))
    remove_empty = all(not line.strip() for line in sections.get('remove', []))

    return add_empty and remove_empty


def run_iwyu_command(iwyu_bin, filtered_clang_cmd, verbose, mapping_file):
    """Run IWYU command and return process result."""
    cmd = ['env', '-u', 'LD_LIBRARY_PATH', iwyu_bin] + filtered_clang_cmd

    cmd.extend(["-Xiwyu", f"--mapping_file={mapping_file}"])
    if verbose:
        cmd.extend(["-Xiwyu", f"--verbose={verbose}"])

    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def process_iwyu_output(iwyu_output, source_root):
    """Process IWYU output by filtering and normalizing paths."""
    normalized_source_root = os.path.normpath(source_root)
    if not normalized_source_root.endswith(os.sep):
        normalized_source_root += os.sep

    filtered_lines = [line for line in iwyu_output.splitlines() if line.strip() != '---']
    iwyu_output = '\n'.join(filtered_lines)
    iwyu_output = filter_facade_to_private_replacements(iwyu_output)

    escaped_source_root = re.escape(normalized_source_root)

    return re.sub(escaped_source_root, "$(SOURCE_ROOT)/", iwyu_output)


def determine_exit_code(iwyu_output, testing_src):
    """Determine appropriate exit code based on IWYU output."""
    iwyu_lower = iwyu_output.lower()
    if any(pattern in iwyu_lower for pattern in ERROR_PATTERNS):
        return 1

    if "should add these lines" in iwyu_output or "should remove these lines" in iwyu_output:
        return 0 if iwyu_changes_empty_simple(iwyu_output, testing_src) else 1

    return 0


def main():
    args, clang_cmd = parse_args()

    if '/retry_cc.py' in str(clang_cmd):
        clang_cmd = list(filter_cmd(clang_cmd))

    generate_outputs(args.iwyu_json)

    if is_generated(args.testing_src, args.build_root):
        return

    filtered_clang_cmd = filter_linker_flags(clang_cmd[1:])

    mapping_file = args.mapping_file if args.mapping_file else args.default_mapping_file

    process = run_iwyu_command(args.iwyu_bin, filtered_clang_cmd, None, mapping_file)
    _, stderr = process.communicate()

    iwyu_output = stderr.decode('utf-8', errors='replace')
    iwyu_output = process_iwyu_output(iwyu_output, args.source_root)

    testing_src = os.path.relpath(args.testing_src, args.source_root)
    exit_code = determine_exit_code(iwyu_output, args.testing_src)

    if args.verbose:
        process = run_iwyu_command(args.iwyu_bin, filtered_clang_cmd, args.verbose, mapping_file)
        _, iwyu_error = process.communicate()
        iwyu_error = iwyu_error.decode('utf-8', errors='replace')
    else:
        iwyu_error = iwyu_output

    result = {
        "file": testing_src,
        "exit_code": exit_code,
        "stderr": iwyu_error,
        "stdout": iwyu_output,
    }

    with open(args.iwyu_json, "w") as f:
        json.dump(result, f, indent=2)


if __name__ == "__main__":
    main()
