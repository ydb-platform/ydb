#!/usr/bin/env python3

import json
import sys
import collections
import base64
import argparse

def gen_chart(ref_time, graph_time, llvm_time = None):
    chars = '▏▎▍▌▋▊▉█'
    fullwidth = chars[-2] # for separators between 1-second blocks

    def draw_line(val, hue, title, comment):
        val = val / 1000.0

        if hue is None:
            color = 'darkgray'
        else:
            color = f'hsl({hue} 80% 40%)'

        out = f'<div style="color:{color};font-family:monospace;">'
        if title:
            out += f'<span style="width:4em;display:inline-block">{title}</span>'

        for _ in range(int(val)):
            out += fullwidth

        remainder = int(round((val - int(val)) * 7.0))
        if remainder > 0:
            out += chars[remainder - 1]
        if comment:
            out += '&nbsp;'
            out += comment
        out += '</div>\n'

        return out

    out = ''
    out += draw_line(ref_time, None, 'C++', None)

    def add_colored_bar(ref_value, result_value, title):
        hue = None
        comment = None
        if result_value:
            shame_ratio = result_value / ref_value
            comment = 'x %.1f' % shame_ratio
            hue = 120 - int(120.0 * (shame_ratio - 0.5) / 1.5) # Map [0.5, 2] -> [120, 0]
            hue = max(0, min(hue, 120)) # clamp to the [0, 120] range; hue 0 = red, hue 120 = green
        return draw_line(result_value, hue, title, comment)

    out += add_colored_bar(ref_time, graph_time, 'Graph')
    if llvm_time is not None:
        out += add_colored_bar(ref_time, llvm_time, 'LLVM')

    return out

def sample_rows(sample):
    return sample['rowsPerRun'] * sample['numRuns']

def format_large_num(num):
    if num >= 1000000:
        sf = 1000000.0
        suffix = 'M'
    elif num >= 1000:
        sf = 1000.0
        suffix = 'K'
    else:
        sf = 1
        suffix = ''

    formatted = '%.02f' % (num / sf)
    if '.' in formatted:
        while formatted.endswith('0'):
            formatted = formatted[:-1]
        if formatted.endswith('.'):
            formatted = formatted[:-1]
    formatted += suffix
    return formatted

def format_time(ms):
    if ms is None:
        return ' '
    return '%.2f' % (ms / 1000.0)

def format_mem(bytez):
    if bytez is None:
        return ' '
    return '%.1f' % (bytez / (1024.0 * 1024.0))

def do_merge_llvm(samples):
    output_samples = []
    for sample in samples:
        if not sample.get('llvm', False):
            output_samples.append(sample)

    sorted_samples = sorted(samples, key=lambda sample: sample.get('llvm', False))
    index = {}

    for sample in sorted_samples:
        is_llvm = sample.get('llvm', False)
        key = (sample['testName'], sample['numKeys'], sample_rows(sample), sample.get('spilling', False), sample.get('blockSize', 0), sample.get('combinerMemLimit', 0), sample['hashType'])
        if key in index and not is_llvm:
            raise Exception('Attempted to merge two non-LLVM result samples, key = %s' % repr(key))
        if key not in index and is_llvm:
            raise Exception('Non-LLVM result sample is missing, key = %s' % repr(key))

        if is_llvm:
            gen_time = sample['generatorTime']
            result_time = sample['resultTime']
            result_time_or_zero = result_time - gen_time if gen_time <= result_time else 0
            index[key]['llvmCleanTime'] = result_time_or_zero
        else:
            index[key] = sample

    return output_samples

def samples_sorted_by_section(samples):
    sort_orders = {
        'testName': {},
        'totalRows': {},
        'spilling': {},
        'blockSize': {},
        'combinerMemLimit': {},
        'hashType': {},
        'numKeys': {},
    }

    for sample in samples:
        for key in sort_orders.keys():
            if key in sample:
                so = sort_orders[key]
                value = sample[key]
                if value not in so:
                    so[value] = len(so)

    def sort_order(sample):
        result = []
        for key in sort_orders.keys():
            if key not in sample:
                result.append(-1)
                continue
            result.append(sort_orders[key][sample[key]])
        return result

    return sorted(samples, key=sort_order)


def friendly_hash_name(hash_type):
    types = {
        'std': 'std::unordered_map',
        'absl': 'absl::flat_hash_map',
    }
    return types[hash_type]


def do_format(merge_llvm):
    per_section = collections.defaultdict(list)

    all_samples = []
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        sample = json.loads(line)
        sample['totalRows'] = sample_rows(sample)
        sample['hashType'] = sample.get('hashType', 'std')
        all_samples.append(sample)

    all_samples = samples_sorted_by_section(all_samples)

    if merge_llvm:
        all_samples = do_merge_llvm(all_samples)

    for sample in all_samples:
        section_name = (sample['testName'], sample_rows(sample), sample.get('llvm', False), sample.get('spilling', False), sample.get('blockSize', 0), sample.get('combinerMemLimit', 0), sample['hashType'])
        per_section[section_name].append(sample)

    for _, samples in per_section.items():
        combiner_name = samples[0]['testName']
        num_rows = sample_rows(samples[0])
        num_rows_formatted = format_large_num(num_rows)

        has_llvm_column = any(('llvmCleanTime' in sample for sample in samples))

        traits = []
        if samples[0].get('llvm', False):
            traits.append('LLVM')
        if samples[0].get('spilling', False):
            traits.append('spilling')
        if samples[0].get('blockSize', 0):
            traits.append(f'{samples[0]["blockSize"]} rows per block')
        memlimit = samples[0].get('combinerMemLimit', 0)
        if memlimit and combiner_name == 'WideCombiner':
            memlimit_formatted = format_mem(memlimit)
            traits.append(f'{memlimit_formatted} MB RAM limit')
        traits.append(f'{num_rows_formatted} input rows')
        hash_name = friendly_hash_name(samples[0]['hashType'])
        traits.append(f'{hash_name}')
        traits_str = ', '.join(traits)

        own_times = []
        for sample in samples:
            own_times.append(sample['generatorTime'])
        own_times.sort()
        median_own_time = format_time(own_times[len(own_times) // 2])

        print(f'##### {combiner_name}, {traits_str}\n')
        print(f'Input generator elapsed time: {median_own_time}с\n')
        print('::: html\n<table><tr>\n')
        headers = [
            'Shame ratio',
            'Distinct keys',
            'Graph time (s)',
            'Reference C++ impl time (s)',
        ]
        if has_llvm_column:
            headers += [
                'LLVM time (s)',
            ]
        headers += [
            'MaxRSS delta, MB',
            'Reference MaxRSS delta, MB',
        ]
        print(''.join(['<th>%s</th>' % item for item in headers]) + '\n')

        for sample in samples:
            gen_time = sample['generatorTime']
            result_time = sample['resultTime']
            ref_time = sample['refTime']
            result_time_or_zero = result_time - gen_time if gen_time <= result_time else 0
            ref_time_or_zero = ref_time - gen_time if gen_time <= ref_time else 0
            llvm_time_or_zero = sample.get('llvmCleanTime', None)

            shame_ratio = 0
            if ref_time_or_zero > 0:
                shame_ratio = result_time_or_zero / ref_time_or_zero

            cols = []
            if llvm_time_or_zero is not None:
                cols.append(gen_chart(ref_time_or_zero, result_time_or_zero, llvm_time_or_zero))
            else:
                cols.append(gen_chart(ref_time_or_zero, result_time_or_zero))

            cols.append(format_large_num(sample['numKeys']))
            cols.append(format_time(result_time_or_zero))
            cols.append(format_time(ref_time_or_zero))
            if has_llvm_column:
                cols.append(format_time(llvm_time_or_zero))
            cols.append(format_mem(sample['maxRssDelta']))
            cols.append(format_mem(sample.get('referenceMaxRssDelta', None)))

            print('<tr>' + ''.join(['<td>%s</td>' % col for col in cols]) + '</tr>\n')

        print('</table>\n:::\n')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--separate-llvm', action='store_true')
    args = parser.parse_args()

    do_format(not args.separate_llvm)

if __name__ == '__main__':
    main()
