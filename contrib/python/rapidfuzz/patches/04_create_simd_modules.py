#!/usr/bin/env python3
"""
Script to create directory structures for *_avx2.pyx files
Based on the pattern observed in rapidfuzz project
Generates ya.make files based on the template with proper VERSION and LICENSE
"""

import os
import sys
import shutil
import re
from pathlib import Path
from typing import List, Tuple


def read_template_file(template_name: str) -> str:
    """
    Read a template file from the patches directory.

    Args:
        template_name: Name of the template file (e.g., 'ya.make.template')

    Returns:
        Template content as string
    """
    template_path = Path(__file__).parent / template_name

    if not template_path.exists():
        raise FileNotFoundError(f"Template file not found at {template_path}")

    with open(template_path, 'r') as f:
        return f.read()


def extract_version_and_license(main_yamake: Path) -> Tuple[str, str]:
    """
    Extract VERSION and LICENSE blocks from main ya.make file.

    Args:
        main_yamake: Path to main ya.make file

    Returns:
        Tuple of (version_line, license_block)
    """
    if not main_yamake.exists():
        raise FileNotFoundError(f"Main ya.make file not found at {main_yamake}")

    with open(main_yamake, 'r') as f:
        content = f.read()

    # Extract VERSION - line 7: VERSION(3.14.3)
    version_match = re.search(r'^VERSION\([^)]+\)$', content, re.MULTILINE)
    if not version_match:
        raise ValueError("VERSION() not found in main ya.make")
    version_line = version_match.group(0)

    # Extract LICENSE block - lines 9-14
    license_match = re.search(
        r'^LICENSE\(\s*\n(?:.*\n)*?\)$',
        content,
        re.MULTILINE
    )
    if not license_match:
        raise ValueError("LICENSE() block not found in main ya.make")
    license_block = license_match.group(0)

    return version_line, license_block


def get_namespace(directory: Path, base_dir: Path) -> str:
    """
    Determine the namespace based on directory structure.

    Args:
        directory: The directory containing the .pyx file
        base_dir: The base project directory

    Returns:
        Namespace string (e.g., "rapidfuzz.distance")
    """
    rel_path = directory.relative_to(base_dir)
    return str(rel_path).replace(os.sep, '.')


def find_avx2_files(base_dir: Path) -> List[Path]:
    """
    Find all *_avx2.pyx files in the project source directories only.
    Excludes files in generated subdirectories (e.g., fuzz_cpp_avx2/, metrics_cpp_avx2/).

    Args:
        base_dir: Base directory to search from

    Returns:
        List of Path objects for found files
    """
    # Recursively find all *_avx2.pyx files, excluding those in *_avx2 directories
    return [
        f for f in base_dir.rglob('*_avx2.pyx')
        if not any(part.endswith('_avx2') for part in f.parent.parts)
    ]


def create_yamake(
    output_path: Path,
    template_content: str,
    version_line: str,
    license_block: str,
    namespace: str,
    pyx_filename: str,
    add_avx2_flag: bool = False
) -> None:
    """
    Create a ya.make file from template with proper replacements.

    Args:
        output_path: Path where ya.make should be created
        template_content: Template content
        version_line: VERSION line from main ya.make
        license_block: LICENSE block from main ya.make
        namespace: Namespace for PY_SRCS
        pyx_filename: Name of the .pyx file
        add_avx2_flag: Whether to add CFLAGS($AVX2_CFLAGS)
    """
    # Replace VERSION() placeholder
    content = re.sub(r'^VERSION\(\)$', version_line, template_content, flags=re.MULTILINE)

    # Replace LICENSE() placeholder
    content = re.sub(r'^LICENSE\(\)$', license_block, content, flags=re.MULTILINE)

    # Replace CFLAGS() placeholder
    if add_avx2_flag:
        cflags_replacement = 'CFLAGS(\n    $AVX2_CFLAGS\n)'
    else:
        # Remove the CFLAGS line entirely for base directories
        cflags_replacement = ''
    content = re.sub(r'^CFLAGS\(\)$', cflags_replacement, content, flags=re.MULTILINE)

    # Replace PY_SRCS() placeholder
    py_srcs_block = f'''PY_SRCS(
    NAMESPACE {namespace}
    CYTHON_CPP
    {pyx_filename}
)'''
    content = re.sub(r'^PY_SRCS\(\)$', py_srcs_block, content, flags=re.MULTILINE)

    # Write the file
    with open(output_path, 'w') as f:
        f.write(content)


def process_avx2_file(
    avx2_file: Path,
    base_dir: Path,
    template_content: str,
    version_line: str,
    license_block: str
) -> str:
    """
    Process a single *_avx2.pyx file and create the necessary directory structure.
    Always recreates directories from scratch.

    Args:
        avx2_file: Path to the _avx2.pyx file
        base_dir: Base project directory
        template_content: Template content for ya.make files
        version_line: VERSION line from main ya.make
        license_block: LICENSE block from main ya.make

    Returns:
        Module name for the base module (e.g., 'rapidfuzz.fuzz_cpp')
    """
    directory = avx2_file.parent
    filename = avx2_file.name

    # Extract base name (without _avx2.pyx)
    base_name = filename.replace('_avx2.pyx', '')

    print(f"Processing: {avx2_file}")

    # Determine namespace
    namespace = get_namespace(directory, base_dir)

    # Create the _avx2 directory (delete if exists, then recreate)
    avx2_dir = directory / f"{base_name}_avx2"
    if avx2_dir.exists():
        shutil.rmtree(avx2_dir)
    print(f"Created: {avx2_dir}")
    avx2_dir.mkdir(parents=True, exist_ok=True)

    # Copy the actual _avx2.pyx file to the _avx2 directory
    avx2_pyx = avx2_dir / f"{base_name}_avx2.pyx"
    shutil.copy2(avx2_file, avx2_pyx)

    # Check if corresponding base .pyx file exists
    base_pyx = directory / f"{base_name}.pyx"
    if not base_pyx.exists():
        print(f"Error: Base file not found: {base_pyx}")
    else:
        # Copy the base .pyx file to the _avx2 directory
        avx2_base_pyx = avx2_dir / f"{base_name}.pyx"
        shutil.copy2(base_pyx, avx2_base_pyx)

    # Create ya.make for _avx2 directory
    avx2_yamake = avx2_dir / "ya.make"
    create_yamake(
        avx2_yamake,
        template_content,
        version_line,
        license_block,
        namespace,
        f"{base_name}_avx2.pyx",
        add_avx2_flag=True
    )

    # Continue processing base directory only if base file exists
    if not base_pyx.exists():
        return

    # Create the base directory (without _avx2 suffix) - delete if exists, then recreate
    base_dir_path = directory / base_name
    if base_dir_path.exists():
        shutil.rmtree(base_dir_path)
    print(f"Created: {base_dir_path}")
    base_dir_path.mkdir(parents=True, exist_ok=True)

    # Copy the actual base .pyx file to the base directory
    base_dir_pyx = base_dir_path / f"{base_name}.pyx"
    shutil.copy2(base_pyx, base_dir_pyx)

    # Create ya.make for base directory
    base_yamake = base_dir_path / "ya.make"
    create_yamake(
        base_yamake,
        template_content,
        version_line,
        license_block,
        namespace,
        f"{base_name}.pyx",
        add_avx2_flag=False
    )

    # Return the module name (namespace + base_name)
    return f"{namespace}.{base_name}"


def generate_test_simd(
    base_dir: Path,
    module_names: List[str]
) -> None:
    """
    Generate tests/test_simd.py from template with the SIMD_MODULES list.

    Args:
        base_dir: Base project directory
        module_names: List of module names to include in SIMD_MODULES
    """
    test_file = base_dir / "tests" / "test_simd.py"

    # Read the template
    template_content = read_template_file('test_simd.py.template')

    # Sort module names for consistent output
    sorted_modules = sorted(module_names)

    # Create the SIMD_MODULES list string
    modules_list = "SIMD_MODULES = [\n"
    for module in sorted_modules:
        modules_list += f"    '{module}',\n"
    modules_list += "]"

    # Replace the SIMD_MODULES list in the template
    # Match SIMD_MODULES = [...] including multiline
    pattern = r'SIMD_MODULES\s*=\s*\[[\s\S]*?\]'
    new_content = re.sub(pattern, modules_list, template_content)

    # Ensure tests directory exists
    test_dir = base_dir / "tests"
    test_dir.mkdir(parents=True, exist_ok=True)

    # Write the generated file
    with open(test_file, 'w') as f:
        f.write(new_content)

    print(f"Generated: {test_file}")


def modify_main_yamake(
    main_yamake: Path,
    module_names: List[str],
    avx2_files: List[Path],
    base_dir: Path
) -> None:
    """
    Modify the main ya.make file to:
    1. Remove base, avx2, and sse2 .pyx files from PY_SRCS (dynamically determined)
    2. Add PEERDIR for base directories in the first PEERDIR block
    3. Add PEERDIR for avx2 directories inside the IF (ARCH_X86_64) block

    Args:
        main_yamake: Path to main ya.make file
        module_names: List of module names (e.g., ['rapidfuzz.fuzz_cpp', 'rapidfuzz.distance.metrics_cpp'])
        avx2_files: List of discovered *_avx2.pyx files
        base_dir: Base project directory
    """
    if not main_yamake.exists():
        raise FileNotFoundError(f"Main ya.make file not found at {main_yamake}")

    with open(main_yamake, 'r') as f:
        lines = f.readlines()

    # Dynamically build files to remove from PY_SRCS based on discovered avx2 files
    files_to_remove = []
    for avx2_file in avx2_files:
        # Get relative path from base_dir
        try:
            rel_path = avx2_file.relative_to(base_dir)
        except ValueError:
            rel_path = avx2_file

        # Extract base name (without _avx2.pyx)
        base_name = avx2_file.stem.replace('_avx2', '')
        directory = avx2_file.parent

        # Get relative directory path
        try:
            rel_dir = directory.relative_to(base_dir)
        except ValueError:
            rel_dir = directory

        # Add the three files to remove: base, avx2, and sse2
        files_to_remove.append(f"{rel_dir}/{base_name}.pyx")
        files_to_remove.append(f"{rel_dir}/{base_name}_avx2.pyx")
        files_to_remove.append(f"{rel_dir}/{base_name}_sse2.pyx")

    # Remove the specified .pyx files from PY_SRCS
    new_lines = []
    for line in lines:
        stripped = line.strip()
        should_remove = any(f in stripped for f in files_to_remove)
        if not should_remove:
            new_lines.append(line)

    lines = new_lines

    # Find the first PEERDIR block (after LICENSE_TEXTS) and add base module peerdirs
    base_peerdirs = []
    for module_name in sorted(module_names):
        # Convert module name to path (e.g., rapidfuzz.fuzz_cpp -> contrib/python/rapidfuzz/rapidfuzz/fuzz_cpp)
        module_path = module_name.replace('.', '/')
        peerdir = f"    contrib/python/rapidfuzz/{module_path}"
        base_peerdirs.append(peerdir)

    # Find the first PEERDIR block and insert base peerdirs
    peerdir_inserted = False
    for i, line in enumerate(lines):
        if 'PEERDIR(' in line and not peerdir_inserted:
            # Find the closing parenthesis of this PEERDIR block
            j = i + 1
            while j < len(lines) and ')' not in lines[j]:
                j += 1
            # Insert base peerdirs before the closing parenthesis
            for peerdir in base_peerdirs:
                lines.insert(j, f"{peerdir}\n")
                j += 1
            peerdir_inserted = True
            break

    # Find the IF (ARCH_X86_64) block and add avx2 peerdirs
    avx2_peerdirs = []
    for module_name in sorted(module_names):
        # Convert module name to path and add _avx2 suffix
        module_path = module_name.replace('.', '/')
        peerdir = f"        contrib/python/rapidfuzz/{module_path}_avx2"
        avx2_peerdirs.append(peerdir)

    # Find IF (ARCH_X86_64) block and add PEERDIR for avx2 modules
    arch_block_found = False
    for i, line in enumerate(lines):
        if 'IF (ARCH_X86_64)' in line:
            arch_block_found = True
            # Insert PEERDIR block after IF (ARCH_X86_64)
            insert_pos = i + 1
            lines.insert(insert_pos, "    PEERDIR(\n")
            insert_pos += 1
            for peerdir in avx2_peerdirs:
                lines.insert(insert_pos, f"{peerdir}\n")
                insert_pos += 1
            lines.insert(insert_pos, "    )\n")
            break

    if not arch_block_found:
        print("Error: IF (ARCH_X86_64) block not found")

    # Write the modified content back
    with open(main_yamake, 'w') as f:
        f.writelines(lines)


def main() -> int:
    """Main entry point for the script"""
    # Get base directory from CLI argument
    if len(sys.argv) < 2:
        print("Usage: python 04_create_simd_modules.py <base_dir>")
        return 1
    base_dir = Path(sys.argv[1])

    # Change to base directory
    os.chdir(base_dir)

    # Paths
    main_yamake = base_dir / "ya.make"

    try:
        # Extract VERSION and LICENSE from main ya.make
        version_line, license_block = extract_version_and_license(main_yamake)

        # Read template from file
        template_content = read_template_file('ya.make.template')

        # Find all *_avx2.pyx files
        avx2_files = find_avx2_files(base_dir)

        if not avx2_files:
            print("No *_avx2.pyx files found.")
            return 0

        # Process each file and collect module names
        module_names = []
        for avx2_file in avx2_files:
            module_name = process_avx2_file(avx2_file, base_dir, template_content, version_line, license_block)
            if module_name:
                module_names.append(module_name)

        # Generate tests/test_simd.py
        if module_names:
            generate_test_simd(base_dir, module_names)

        # Modify main ya.make file
        if module_names:
            modify_main_yamake(main_yamake, module_names, avx2_files, base_dir)

        return 0

    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

