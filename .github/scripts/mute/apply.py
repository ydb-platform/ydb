"""Apply mute/unmute/delete logic and write output files."""
import logging
import os
from collections import defaultdict

from .logic import (
    create_debug_string,
    create_test_string,
    get_wildcard_delete_candidates,
    get_wildcard_unmute_candidates,
    is_chunk_test,
    is_delete_candidate,
    is_mute_candidate,
    is_unmute_candidate,
)


def sort_key_without_prefix(line):
    """Sort key ignoring +++, ---, xxx prefixes."""
    if line.startswith('+++ ') or line.startswith('--- ') or line.startswith('xxx '):
        return line[4:]
    return line


def add_lines_to_file(file_path, lines_to_add):
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w') as f:
            f.writelines(lines_to_add)
        logging.info(f"Lines added to {file_path}")
    except Exception as e:
        logging.error(f"Error adding lines to {file_path}: {e}")


def create_file_set(aggregated_for_mute, filter_func, mute_check=None, use_wildcards=False, resolution=None, exclude_check=None):
    """Create set of tests for file based on filter."""
    result_set = set()
    debug_list = []
    total = len(aggregated_for_mute)
    last_percent = -1
    for idx, test in enumerate(aggregated_for_mute, 1):
        testsuite = test.get('suite_folder')
        testcase = test.get('test_name')

        if not testsuite or not testcase:
            continue

        if exclude_check and exclude_check(testsuite, testcase):
            continue
        if mute_check and not mute_check(testsuite, testcase):
            continue
        percent = int(idx / total * 100)
        if percent != last_percent and (percent % 5 == 0 or percent == 100):
            print(f"\r[create_file_set] Progress: {percent}% ({idx}/{total})", end="")
            last_percent = percent
        if filter_func(test):
            test_string = create_test_string(test, use_wildcards)
            result_set.add(test_string)
            if resolution:
                debug_string = create_debug_string(
                    test,
                    period_days=test.get('period_days'),
                    date_window=test.get('date_window')
                )
                debug_list.append(debug_string)
    if last_percent != 100:
        print(f"\r[create_file_set] Progress: 100% ({total}/{total})", end="")
    print()
    return sorted(list(result_set)), sorted(debug_list)


def write_file_set(file_path, test_set, debug_list=None, sort_without_prefixes=False):
    if not sort_without_prefixes:
        sorted_test_set = sorted(test_set)
    else:
        sorted_test_set = sorted(test_set, key=sort_key_without_prefix)

    sorted_debug_list = None
    if debug_list:
        if not sort_without_prefixes:
            sorted_debug_list = sorted(debug_list)
        else:
            sorted_debug_list = sorted(debug_list, key=sort_key_without_prefix)

    add_lines_to_file(file_path, [line + '\n' for line in sorted_test_set])
    if sorted_debug_list:
        debug_path = file_path.replace('.txt', '_debug.txt')
        add_lines_to_file(debug_path, [line + '\n' for line in sorted_debug_list])
    logging.info(f"Created {os.path.basename(file_path)} with {len(sorted_test_set)} tests")


def apply_and_add_mutes(
    all_data,
    output_path,
    mute_check,
    aggregated_for_mute,
    aggregated_for_unmute,
    aggregated_for_delete,
    quarantine_check=None,
    quarantine_path=None,
    to_graduated=None,
    mute_rule_params=None,
    unmute_rule_params=None,
    delete_rule_params=None,
    output_file=None,
):
    output_path = os.path.join(output_path, 'mute_update')
    to_graduated = to_graduated or set()
    mute_rule_params = mute_rule_params or {}
    unmute_rule_params = unmute_rule_params or {}
    delete_rule_params = delete_rule_params or {}
    logging.info(f"Creating mute files in directory: {output_path}")

    muted_tests = [test for test in aggregated_for_mute if test.get('is_muted', False)]
    logging.info(f"Total muted tests found: {len(muted_tests)}")

    try:
        def is_mute_candidate_wrapper(test):
            return is_mute_candidate(test, aggregated_for_mute, mute_rule_params)

        to_mute, to_mute_debug = create_file_set(
            aggregated_for_mute, is_mute_candidate_wrapper, use_wildcards=True, resolution='to_mute',
            exclude_check=quarantine_check
        )
        write_file_set(os.path.join(output_path, 'to_mute.txt'), to_mute, to_mute_debug)

        def is_unmute_candidate_wrapper(test):
            if is_chunk_test(test):
                return False
            return is_unmute_candidate(test, aggregated_for_unmute, unmute_rule_params)

        def _is_unmute_for_wildcard(test, agg):
            return is_unmute_candidate(test, agg, unmute_rule_params)

        to_unmute, to_unmute_debug = create_file_set(
            aggregated_for_unmute, is_unmute_candidate_wrapper, mute_check, resolution='to_unmute'
        )
        wildcard_unmute = get_wildcard_unmute_candidates(aggregated_for_unmute, mute_check, _is_unmute_for_wildcard)
        wildcard_unmute_patterns = [p for p, d in wildcard_unmute]
        wildcard_unmute_debugs = [d for p, d in wildcard_unmute]
        to_unmute = sorted(list(set(to_unmute) | set(wildcard_unmute_patterns)))
        to_unmute_debug = sorted(list(set(to_unmute_debug) | set(wildcard_unmute_debugs)))
        write_file_set(os.path.join(output_path, 'to_unmute.txt'), to_unmute, to_unmute_debug)

        def is_delete_candidate_wrapper(test):
            if is_chunk_test(test):
                return False
            return is_delete_candidate(test, aggregated_for_delete, delete_rule_params)

        def _is_delete_for_wildcard(test, agg):
            return is_delete_candidate(test, agg, delete_rule_params)

        to_delete, to_delete_debug = create_file_set(
            aggregated_for_delete, is_delete_candidate_wrapper, mute_check, resolution='to_delete'
        )
        wildcard_delete = get_wildcard_delete_candidates(aggregated_for_delete, mute_check, _is_delete_for_wildcard)
        wildcard_delete_patterns = [p for p, d in wildcard_delete]
        wildcard_delete_debugs = [d for p, d in wildcard_delete]
        to_delete = sorted(list(set(to_delete) | set(wildcard_delete_patterns)))
        to_delete_debug = sorted(list(set(to_delete_debug) | set(wildcard_delete_debugs)))
        write_file_set(os.path.join(output_path, 'to_delete.txt'), to_delete, to_delete_debug)

        all_muted_ya, all_muted_ya_debug = create_file_set(
            all_data, lambda test: mute_check(test.get('suite_folder'), test.get('test_name')) if mute_check else True, use_wildcards=True, resolution='muted_ya'
        )
        write_file_set(os.path.join(output_path, 'muted_ya.txt'), all_muted_ya, all_muted_ya_debug)
        to_mute_set = set(to_mute)
        to_unmute_set = set(to_unmute)
        to_delete_set = set(to_delete)
        all_muted_ya_set = set(all_muted_ya)

        test_debug_dict = {}
        wildcard_to_chunks = defaultdict(list)
        for test in aggregated_for_mute + aggregated_for_unmute + aggregated_for_delete:
            test_str = create_test_string(test, use_wildcards=False)
            debug_str = create_debug_string(test)
            test_debug_dict[test_str] = debug_str
            if is_chunk_test(test):
                wildcard_key = create_test_string(test, use_wildcards=True)
                wildcard_to_chunks[wildcard_key].append(test)
        for wildcard, chunks in wildcard_to_chunks.items():
            N = len(chunks)
            m = sum(1 for t in chunks if t.get('is_muted'))
            total_pass = sum(t.get('pass_count', 0) for t in chunks)
            total_fail = sum(t.get('fail_count', 0) for t in chunks)
            total_mute = sum(t.get('mute_count', 0) for t in chunks)
            total_skip = sum(t.get('skip_count', 0) for t in chunks)
            total_runs = total_pass + total_fail + total_mute + total_skip
            reason = ""
            if wildcard in to_mute_set:
                reason = f"TO_MUTE: {total_fail} fails in {total_runs} runs"
            elif wildcard in to_unmute_set:
                reason = f"TO_UNMUTE: {total_fail + total_mute} fails in {total_runs} runs"
            elif wildcard in to_delete_set:
                reason = f"TO_DELETE: {total_runs} total runs"
            debug_str = f"{wildcard}: {N} chunks, {m} muted ({reason})"
            test_debug_dict[wildcard] = debug_str

        muted_ya_plus_to_mute = list(all_muted_ya) + [t for t in to_mute if t not in all_muted_ya]
        muted_ya_plus_to_mute_debug = [test_debug_dict.get(t, "NO DEBUG INFO") for t in muted_ya_plus_to_mute]
        write_file_set(os.path.join(output_path, 'muted_ya+to_mute.txt'), muted_ya_plus_to_mute, muted_ya_plus_to_mute_debug)

        muted_ya_minus_to_unmute = [t for t in all_muted_ya if t not in to_unmute]
        muted_ya_minus_to_unmute_debug = [test_debug_dict.get(t, "NO DEBUG INFO") for t in muted_ya_minus_to_unmute]
        write_file_set(os.path.join(output_path, 'muted_ya-to_unmute.txt'), muted_ya_minus_to_unmute, muted_ya_minus_to_unmute_debug)

        muted_ya_minus_to_delete = [t for t in all_muted_ya if t not in to_delete]
        muted_ya_minus_to_delete_debug = [test_debug_dict.get(t, "NO DEBUG INFO") for t in muted_ya_minus_to_delete]
        write_file_set(os.path.join(output_path, 'muted_ya-to_delete.txt'), muted_ya_minus_to_delete, muted_ya_minus_to_delete_debug)

        muted_ya_minus_to_delete_to_unmute = [t for t in all_muted_ya if t not in to_delete and t not in to_unmute and t not in to_graduated]
        muted_ya_minus_to_delete_to_unmute_debug = [test_debug_dict.get(t, "NO DEBUG INFO") for t in muted_ya_minus_to_delete_to_unmute]
        write_file_set(os.path.join(output_path, 'muted_ya-to-delete-to-unmute.txt'), muted_ya_minus_to_delete_to_unmute, muted_ya_minus_to_delete_to_unmute_debug)

        muted_ya_minus_to_delete_to_unmute_plus_to_mute = list(muted_ya_minus_to_delete_to_unmute) + [t for t in to_mute if t not in muted_ya_minus_to_delete_to_unmute]
        muted_ya_minus_to_delete_to_unmute_plus_to_mute_debug = [test_debug_dict.get(t, "NO DEBUG INFO") for t in muted_ya_minus_to_delete_to_unmute_plus_to_mute]
        final_output = output_file or 'new_muted_ya.txt'
        write_file_set(os.path.join(output_path, final_output), muted_ya_minus_to_delete_to_unmute_plus_to_mute, muted_ya_minus_to_delete_to_unmute_plus_to_mute_debug)

        all_test_strings = sorted(all_muted_ya_set | to_mute_set | to_unmute_set | to_delete_set, key=sort_key_without_prefix)
        muted_ya_changes = []
        muted_ya_changes_debug = []
        for test_str in all_test_strings:
            if test_str in to_mute_set and test_str not in all_muted_ya_set:
                prefix = "+++"
            elif test_str in to_unmute_set:
                prefix = "---"
            elif test_str in to_delete_set:
                prefix = "xxx"
            else:
                prefix = ""
            line = f"{prefix} {test_str}" if prefix else f"{test_str}"
            muted_ya_changes.append(line)
            debug_val = test_debug_dict.get(test_str, "NO DEBUG INFO")
            muted_ya_changes_debug.append(f"{prefix} {debug_val}" if prefix else debug_val)
        write_file_set(os.path.join(output_path, 'muted_ya_changes.txt'), muted_ya_changes, muted_ya_changes_debug, sort_without_prefixes=True)

        logging.info(f"To mute: {len(to_mute)}, To unmute: {len(to_unmute)}, To delete: {len(to_delete)}")
        return (
            to_mute, to_unmute, to_delete,
            to_mute_debug, to_unmute_debug, to_delete_debug,
        )

    except (KeyError, TypeError) as e:
        logging.error(f"Error processing test data: {e}")
        return ([], [], [], [], [], [])

    return ([], [], [], [], [], [])
