"""
Code for `sarif ls` command.
"""

from typing import List

from sarif import loader


def print_ls(files_or_dirs: List[str], output):
    """
    Print a SARIF file listing for each of the input files or directories.
    """
    dir_result = []
    for path in files_or_dirs:
        dir_result.append(f"{path}:")
        sarif_files = loader.load_sarif_files(path)
        if sarif_files:
            sarif_file_names = [f.get_file_name() for f in sarif_files]
            for file_name in sorted(sarif_file_names):
                dir_result.append(f"  {file_name}")
        else:
            dir_result.append("  (None)")
    if output:
        print("Writing file listing to", output)
        with open(output, "w", encoding="utf-8") as file_out:
            file_out.writelines(d + "\n" for d in dir_result)
    else:
        for directory in dir_result:
            print(directory)
        print()
