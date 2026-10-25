import bz2
import json
import struct
import subprocess
import tempfile
import unittest
from pathlib import Path

import yatest.common


# PickSubgroup for TLogoBlobID(1, 1, 1, 0, 100, 0); subgroup node % 3 is its DC.
SUBGROUP_ORDER = (7, 1, 4, 8, 2, 5, 6, 0, 3)
CHANNEL5_SUBGROUP_ORDER = (0, 3, 6, 1, 4, 7, 2, 5, 8)
BLOCK42_SUBGROUP_ORDER = (5, 6, 7, 0, 1, 2, 3, 4)
TWO_REALMS = tuple(index for node, index in enumerate(SUBGROUP_ORDER) if node % 3 != 2)


def ingress_bit(position):
    # Generic ingress numbers bits from the high bit of each byte.
    return 1 << (position // 8 * 8 + 7 - position % 8)


class Snapshot:
    """Build and display snapshot records using the generic ingress format.

    VDisk indexes are group order numbers; known nodes are subgroup positions.
    Part indexes are zero-based. Barrier keys contain tablet, channel/hard bits,
    record generation and counter; values contain collect generation/step and ingress.
    """

    def __init__(self, local_realms=(0,), keep=0, barrier=None, quorum=True, known_full=True, indexes=TWO_REALMS,
                 known_nodes=None, local_parts=None, keep_flags=None, blob_indexes=None, barrier_indexes=None,
                 barriers=None, known_per_disk=None, subgroup_order=SUBGROUP_ORDER, part_count=3, blob_channel=0):
        self.parameters = dict(local_realms=tuple(local_realms), keep=keep, barrier=barrier, quorum=quorum,
                               known_full=known_full, indexes=tuple(indexes))
        for name, value in dict(known_nodes=known_nodes, local_parts=local_parts, keep_flags=keep_flags,
                                blob_indexes=blob_indexes, barrier_indexes=barrier_indexes,
                                barriers=barriers, known_per_disk=known_per_disk).items():
            if value is not None:
                self.parameters[name] = value
        if subgroup_order != SUBGROUP_ORDER:
            self.parameters['subgroup_order'] = subgroup_order
        if part_count != 3:
            self.parameters['part_count'] = part_count
        if blob_channel:
            self.parameters['blob_channel'] = blob_channel

        if known_nodes is None:
            known_nodes = tuple(range(len(subgroup_order))) if known_full else (0,)
        if barriers is None:
            barriers = [] if barrier is None else [dict(kind=barrier)]
        self.records = []
        for index in self.parameters['indexes']:
            node = subgroup_order.index(index)
            part = node % part_count
            known = 0
            for known_node in (known_per_disk or {}).get(index, known_nodes):
                known_part = known_node % part_count
                position = (2 + known_part if known_node < part_count else
                            2 + 2 * part_count + (known_node - part_count) * 2 * part_count + known_part * 2 + 1)
                known |= ingress_bit(position)
            parts = (local_parts or {}).get(index, (part,) if part in local_realms else ())
            local = sum(ingress_bit(2 + part_count + part) for part in parts)
            flags = (keep_flags or {}).get(index, keep)
            if isinstance(flags, int):
                flags = (flags,)

            # TLogoBlobID(tablet=1, generation=1, step=1, channel=blob_channel, size=100).
            blob_key = (1, 1 << 24 | blob_channel << 56, 100 << 4 | 1 << 56)
            blobs = []
            if blob_indexes is None or index in blob_indexes:
                blobs = [(blob_key, known | local | flag << 6) for flag in flags]
            barrier_records = []
            if barrier_indexes is None or index in barrier_indexes:
                for parameters in barriers:
                    fields = dict(kind='soft', record=(2, 1), collect=(1, 1), tablet=1, channel=blob_channel,
                                  ingress=(1 << len(subgroup_order)) - 1 if quorum else 1 << index)
                    fields.update(parameters)
                    key = (fields['tablet'], fields['channel'] | int(fields['kind'] == 'hard') << 31,
                           *fields['record'])
                    value = (*fields['collect'], fields['ingress'])
                    barrier_records.append((key, value))
            self.records.append((index, node, blobs, barrier_records))

    def __repr__(self):
        parameters = ', '.join(f'{name}={value!r}' for name, value in self.parameters.items())
        lines = [f'Snapshot({parameters})']
        for index, node, blobs, barriers in self.records:
            blob_text = ', '.join(f'(key=({", ".join(hex(word) for word in key)}), ingress={ingress:#018x})'
                                  for key, ingress in blobs)
            barrier_text = ', '.join(f'(key={key}, value=({value[0]}, {value[1]}, {value[2]:#05x}))'
                                     for key, value in barriers)
            lines.append(f'  VDisk(index={index}, subgroup={node}, blobs=[{blob_text}], barriers=[{barrier_text}])')
        return '\n'.join(lines)

    def __bytes__(self):
        records = []
        for index, node, blobs, barriers in self.records:
            blocks = b''
            if blobs:
                blocks += struct.pack('<IIQ', len(blobs), 0, 0)
                for key, ingress in blobs:
                    blocks += struct.pack('<QQQQ', *key, ingress)
            if barriers:
                blocks += struct.pack('<IIQ', len(barriers), 2, 0)
                for key, value in barriers:
                    blocks += struct.pack('<QIII', *key) + struct.pack('<III', *value)
            payload = bz2.compress(struct.pack('<QQQQ', 1, 1, 1, bool(blobs) + bool(barriers)) + blocks)
            records.append(struct.pack('<III', 1, index, len(payload)) + payload)
        return b''.join(records)


def snapshot(local_realms=(0,), keep=0, barrier=None, quorum=True, known_full=True, indexes=TWO_REALMS, **parameters):
    return Snapshot(local_realms, keep, barrier, quorum, known_full, indexes, **parameters)


class TestBlobSan:
    def _canonical_file(self, filename, content):
        path = yatest.common.output_path(filename)
        Path(path).write_text(content)
        return yatest.common.canonical_file(path, local=True, universal_lines=True)

    def _trace(self, arguments, data=b'', capture_stdout=False):
        binary = yatest.common.binary_path('ydb/tools/blobsan/blobsan')
        result = subprocess.run(
            [binary, *arguments], input=bytes(data),
            stdout=subprocess.PIPE if capture_stdout else subprocess.DEVNULL,
            stderr=subprocess.PIPE, check=True,
        )
        lines = [' '.join(['blobsan', *arguments]), f'Exit Status: {result.returncode}']
        if result.stdout:
            lines.append('Stdout: ' + result.stdout.decode().replace(binary, 'blobsan').rstrip('\n'))
        if result.stderr:
            lines.append('Stderr: ' + result.stderr.decode().replace(binary, 'blobsan').rstrip('\n'))
        return '\n'.join(lines)

    def run_blobsan(self, data=b'', options=()):
        return self._trace([*options, 'mirror-3-dc'], data)

    def _testcase(self, name, data, description):
        results = [f'===== testcase =====\n{name}\n{description}\nSnapshot: {data!r}']
        results.extend(self.run_blobsan(data, options) for options in [(), ('--write2x2-candidates',)])
        return '\n\n'.join(results)

    def test_stdout_dump(self):
        data = snapshot(keep=1, barrier='soft', local_realms=(0, 1))
        result = f'Snapshot: {data!r}\n\n' + self._trace(['mirror-3-dc'], data, capture_stdout=True)
        return self._canonical_file('output.txt', result + '\n')

    def test_barrier_reporting(self):
        cases = [
            ('Keep flags use subgroup positions', {
                'barrier': 'soft', 'indexes': tuple(range(9)), 'keep_flags': {2: 1, 8: 1},
            },
                'When only VDisks 2 and 8 have Keep,\n'
                'then only their soft barrier reports contain (keep), regardless of subgroup order.'),
            ('channel greater than collected step', {
                'keep': 1, 'barrier': 'soft', 'indexes': tuple(range(9)),
                'blob_channel': 5, 'subgroup_order': CHANNEL5_SUBGROUP_ORDER,
            },
                'When a channel-5 blob at step 1 is covered by a barrier collecting through step 1,\n'
                'then every barrier report contains (collect)(keep).'),
            ('step greater than collected step', {'barriers': [{'collect': (1, 0)}]},
             'When a channel-0 blob at step 1 is above a barrier collecting through step 0,\n'
             'then its barrier reports contain neither (collect) nor (keep).'),
        ]
        results = [self._testcase(name, snapshot(**parameters), description)
                   for name, parameters, description in cases]
        return self._canonical_file('output.txt', '\n\n'.join(results) + '\n')

    def test_block42(self):
        cases = [
            ('full local layout', 6, 'soft', 'A kept blob with all six main parts is complete and is skipped.'),
            ('incomplete local layout', 4, 'soft', 'Four local parts are restorable but incomplete, so the kept blob is reported.'),
            ('too few local parts', 3, 'soft', 'Three local parts are not restorable and contribute to TrashSize instead.'),
            ('above barriers', 4, None, 'An incomplete blob above all barriers is skipped in default mode.'),
        ]
        results = []
        for name, num_parts, barrier, description in cases:
            local_parts = {BLOCK42_SUBGROUP_ORDER[node]: (node,) for node in range(num_parts)}
            data = snapshot(keep=1, barrier=barrier, indexes=tuple(range(8)),
                            subgroup_order=BLOCK42_SUBGROUP_ORDER, part_count=6, known_nodes=tuple(range(6)),
                            local_realms=(), local_parts=local_parts)
            results.append(f'===== testcase =====\n{name}\n{description}\nSnapshot: {data!r}\n\n'
                           + self._trace(['block-4-2'], data))
        return self._canonical_file('output.txt', '\n\n'.join(results) + '\n')

    def test_write2x2_candidates(self):
        cases = [
            ('above barriers', {},
             'When a blob has no GC barrier and local copies exist in only one of the two captured DCs,\n'
             'then default mode skips it; write2x2 mode reports a candidate with FULL because ingress claims a full layout.'),
            ('keep above barriers', {'keep': 1},
             'When a blob has Keep but no GC barrier and local copies exist in only one captured DC,\n'
             'then default mode skips it despite Keep; write2x2 mode reports a candidate with FULL.'),
            ('missing DC inferred from ingress', {'keep': 1, 'barrier': 'soft'},
             'When a kept blob is at a confirmed soft barrier and one DC is missing from the snapshot,\n'
             'then default mode accepts the full layout inferred from ingress; write2x2 mode reports a candidate with FULL.'),
            ('all disks captured, local data incomplete',
             {'keep': 1, 'barrier': 'soft', 'indexes': range(9)},
             'When all nine VDisks are captured but a kept blob at the soft barrier has local copies in only one DC,\n'
             'then both modes report a candidate; only write2x2 mode appends FULL to indicate full ingress.'),
            ('local data full in two DCs', {'keep': 1, 'barrier': 'soft', 'local_realms': (0, 1)},
             'When a kept blob at the soft barrier has three local replicas in each of two captured DCs,\n'
             'then both modes consider the local layout full and report no candidate.'),
            ('local data full, ingress incomplete',
             {'keep': 1, 'barrier': 'soft', 'local_realms': (0, 1), 'known_full': False},
             'When local replicas form a full layout in two DCs but ingress knows only one main part,\n'
             'then default mode reports the kept blob because ingress is incomplete; write2x2 mode reports no candidate.'),
            ('DoNotKeep above barriers', {'keep': 2},
             'When a blob has DoNotKeep and no GC barrier,\n'
             'then both modes exclude it from recovery candidates.'),
            ('DoNotKeep overrides Keep', {'keep': 3},
             'When a blob ingress contains both Keep and DoNotKeep,\n'
             'then DoNotKeep takes precedence and both modes exclude the blob from recovery candidates.'),
            ('soft collected without Keep', {'barrier': 'soft'},
             'When a blob is exactly at a confirmed soft barrier and has no Keep flag,\n'
             'then both modes discard its local parts and exclude it from recovery candidates.'),
            ('hard collected with Keep', {'keep': 1, 'barrier': 'hard'},
             'When a blob is exactly at a confirmed hard barrier and has Keep,\n'
             'then the hard barrier takes precedence over Keep and both modes exclude the blob.'),
            ('hard barrier without quorum', {'keep': 1, 'barrier': 'hard', 'quorum': False},
             'When a kept blob is covered by a hard barrier whose ingress lacks quorum,\n'
             'then default mode ignores the barrier and skips the blob above any accepted barrier; write2x2 mode excludes it as hard-collected.'),
            ('empty snapshot', {'indexes': ()},
             'When the input contains no VDisk snapshots,\n'
             'then both modes exit successfully without candidates or trash diagnostics.'),
            ('no local parts with all disks captured', {
                'keep': 1, 'barrier': 'soft', 'local_realms': (), 'indexes': tuple(range(9)),
            },
                'When all nine VDisks contain metadata for a kept blob at the soft barrier but none has local data,\n'
                'then both modes exclude the blob through the phantom filter because no local part is available.'),
            ('no local parts with missing DC', {'keep': 1, 'barrier': 'soft', 'local_realms': ()},
             'When captured VDisks have no local data for a kept blob but ingress claims parts in an uncaptured DC,\n'
             'then both modes report a candidate using the inferred parts for the phantom check; write2x2 mode also appends FULL.'),
            ('incomplete ingress and local data', {'keep': 1, 'barrier': 'soft', 'known_full': False},
             'When a kept blob at the soft barrier has incomplete local data and ingress knows only one main part,\n'
             'then both modes report a candidate and neither appends FULL.'),
            ('no known parts', {'keep': 1, 'barrier': 'soft', 'known_nodes': ()},
             'When three local replicas exist but ingress knows no parts for a kept blob at the soft barrier,\n'
             'then both modes exclude the blob through the phantom filter and report TrashSize# 300.'),
            ('full local data in three DCs', {
                'keep': 1, 'barrier': 'soft', 'local_realms': (0, 1, 2), 'indexes': tuple(range(9)),
            },
                'When all nine VDisks are captured and every DC has all three local replicas,\n'
                'then both modes recognize a full layout and report no candidate.'),
            ('one replica in each DC', {
                'keep': 1, 'barrier': 'soft', 'indexes': tuple(range(9)),
                'local_realms': (), 'local_parts': {7: (0,), 1: (1,), 4: (2,)},
            },
                'When exactly one local replica is present in each of the three DCs,\n'
                'then the mirror-3-dc layout is full and both modes report no candidate.'),
            ('two replicas in each of two DCs', {
                'keep': 1, 'barrier': 'soft', 'local_realms': (), 'local_parts': {7: (0,), 8: (0,), 1: (1,), 2: (1,)},
            },
                'When exactly two local replicas are present in each of two captured DCs,\n'
                'then the two-by-two layout is full and both modes report no candidate.'),
            ('one replica missing from two-by-two layout', {
                'keep': 1, 'barrier': 'soft', 'local_realms': (), 'local_parts': {7: (0,), 8: (0,), 1: (1,)},
            },
                'When two local replicas exist in one captured DC and only one exists in the other,\n'
                'then default mode accepts the layout after inferring the missing DC; write2x2 mode reports a candidate with FULL.'),
            ('misplaced local part', {'keep': 1, 'barrier': 'soft', 'local_parts': {7: (1,)}},
             'When one local part belongs to the wrong DC and the remaining local copies are incomplete,\n'
             'then both modes report TrashSize# 100 for the misplaced part; only write2x2 mode also reports a candidate with FULL.'),
            ('Keep on one disk, other local parts discarded', {'barrier': 'soft', 'keep_flags': {7: 1}},
             'When only one VDisk has Keep for a blob covered by a confirmed soft barrier,\n'
             'then local parts on other captured VDisks are discarded and both modes report a candidate using the remaining Keep.'),
            ('DoNotKeep on one disk overrides Keep elsewhere', {'keep': 1, 'keep_flags': {7: 2}},
             'When one VDisk has DoNotKeep while the other captured VDisks have Keep,\n'
             'then the merged DoNotKeep overrides Keep and both modes exclude the blob.'),
            ('Keep merged from duplicate blob records', {'barrier': 'soft', 'keep_flags': {7: (0, 1)}},
             'When one VDisk has two records for the same blob, first without flags and then with Keep,\n'
             'then Keep survives record merging and both modes report the kept blob with incomplete local data.'),
            ('DoNotKeep merged from duplicate blob records', {'keep': 1, 'keep_flags': {7: (1, 2)}},
             'When one VDisk has duplicate records containing Keep and DoNotKeep separately,\n'
             'then DoNotKeep wins during record merging and both modes exclude the blob.'),
            ('blob absent from a captured disk', {'keep': 1, 'barrier': 'soft', 'blob_indexes': TWO_REALMS[1:]},
             'When a captured VDisk has no record for a blob that is present on the other captured VDisks,\n'
             'then default mode accepts the inferred full layout; write2x2 mode excludes the phantom-like blob and reports estimated TrashSize# 500.'),
            ('two DCs missing', {'keep': 1, 'barrier': 'soft', 'indexes': (7, 8, 6)},
             'When only the three VDisks of one DC are captured for a kept blob at the soft barrier,\n'
             'then default mode accepts inferred completeness; write2x2 mode rejects coverage outside the failure model and reports estimated TrashSize# 900.'),
            ('snapshot has no blob records', {'blob_indexes': ()},
             'When VDisk streams are present but contain no blob or barrier records,\n'
             'then both modes exit successfully without candidates or trash diagnostics.'),
            ('some disks have incomplete ingress', {
                'keep': 1, 'barrier': 'soft', 'indexes': tuple(range(9)), 'known_per_disk': {7: (0,), 8: (0,), 6: (0,)},
            },
                'When the DC with local data has incomplete ingress while the other captured VDisks provide full ingress,\n'
                'then ingress merges into a full layout but local data remains incomplete, so both modes report a candidate.'),
            ('full local data without reflected parts', {
                'keep': 1, 'barrier': 'soft', 'local_realms': (0, 1), 'indexes': tuple(range(9)), 'known_per_disk': {
                    index: tuple(node for node in range(9) if node != own_node)
                    for own_node, index in enumerate(SUBGROUP_ORDER)
                },
            },
                'When local data forms a full layout but no VDisk ingress reflects its own part,\n'
                'then default mode reports the missing reflection information; write2x2 mode reports no candidate because local data is full.'),
            ('blob above soft barrier step', {'barriers': [{'collect': (1, 0)}]},
             'When the blob generation matches the soft barrier but its step is greater than the collect step,\n'
             'then default mode skips the blob above the barrier; write2x2 mode checks it and reports a candidate with FULL.'),
            ('blob above soft barrier generation', {'barriers': [{'collect': (0, 100)}]},
             'When the blob generation is greater than the soft barrier collect generation,\n'
             'then default mode skips the blob above the barrier; write2x2 mode checks it and reports a candidate with FULL.'),
            ('blob below soft barrier generation', {'barriers': [{'collect': (2, 0)}]},
             'When a blob without Keep has a generation below the soft barrier collect generation,\n'
             'then both modes treat it as soft-collected and report no candidate.'),
            ('barrier for another tablet', {'barriers': [{'tablet': 2}]},
             'When the only GC barrier belongs to a different tablet,\n'
             'then both modes ignore that barrier; default mode skips the blob and write2x2 mode reports a candidate with FULL.'),
            ('barrier for another channel', {'barriers': [{'channel': 1}]},
             'When the only GC barrier belongs to a different channel of the same tablet,\n'
             'then both modes ignore that barrier; default mode skips the blob and write2x2 mode reports a candidate with FULL.'),
            ('hard barrier on one disk', {'keep': 1, 'barrier': 'hard', 'barrier_indexes': (7,)},
             'When only one captured VDisk has a confirmed hard barrier covering the kept blob,\n'
             'then that hard barrier excludes the blob from recovery candidates in both modes.'),
            ('blob covered by soft barrier but above hard barrier', {'keep': 1, 'barriers': [
                {'kind': 'soft'}, {'kind': 'hard', 'record': (3, 1), 'collect': (1, 0)},
            ]},
                'When a kept blob is covered by the soft barrier but lies above the hard barrier collect step,\n'
                'then the hard barrier does not collect it; default mode accepts inferred completeness and write2x2 mode reports a candidate with FULL.'),
        ]
        results = [self._testcase(name, snapshot(**parameters), description)
                   for name, parameters, description in cases]
        return self._canonical_file('output.txt', '\n\n'.join(results) + '\n')

    def test_barrier_selection(self):
        cases = [
            ('soft barrier without quorum', {'keep': 1, 'barrier': 'soft', 'quorum': False},
             'When a kept blob has incomplete local data and a soft barrier without quorum,\n'
             'then default mode ignores the barrier and skips the blob; write2x2 mode accepts it and reports a candidate under the barrier with FULL.'),
            ('soft barrier without quorum and without Keep', {'barrier': 'soft', 'quorum': False},
             'When a blob without Keep is covered by a soft barrier whose ingress lacks quorum,\n'
             'then default mode skips it above any accepted barrier; write2x2 mode accepts the barrier and excludes the soft-collected blob.'),
            ('barrier ingress fragments form quorum', {'keep': 1, 'indexes': tuple(range(9)), 'barriers': [
                {'ingress': 0x007}, {'ingress': 0x038},
            ]},
                'When two records for the same barrier carry ingress 0x007 and 0x038,\n'
                'then their merged ingress 0x03f forms quorum, so both modes accept the barrier and report the kept candidate.'),
            ('duplicate barrier ingress still has no quorum', {'keep': 1, 'indexes': tuple(range(9)), 'barriers': [
                {'ingress': 0x007}, {'ingress': 0x007},
            ]},
                'When two records for the same barrier both carry ingress 0x007,\n'
                'then merging leaves 0x007 without quorum; default mode skips the blob and write2x2 mode reports the kept candidate.'),
            ('newer barrier lacks quorum', {'keep': 1, 'indexes': tuple(range(9)), 'barriers': [
                {}, {'record': (3, 1), 'collect': (1, 2), 'ingress': 0},
            ]},
                'When confirmed barrier record (2, 1) is followed by unconfirmed record (3, 1),\n'
                'then default mode selects (2, 1) collecting through (1, 1); write2x2 mode selects (3, 1) collecting through (1, 2); both report a candidate.'),
            ('older barrier arrives after newer barrier', {'keep': 1, 'indexes': tuple(range(9)), 'barriers': [
                {'record': (3, 1), 'collect': (1, 2)}, {},
            ]},
                'When confirmed record (3, 1) arrives before older confirmed record (2, 1),\n'
                'then both modes retain (3, 1) collecting through (1, 2) and report the kept candidate.'),
            ('barrier counters order records within generation', {'keep': 1, 'indexes': tuple(range(9)), 'barriers': [
                {'record': (2, 2), 'collect': (1, 2)}, {},
            ]},
                'When confirmed records share generation 2 and arrive with counters 2 then 1,\n'
                'then both modes retain record (2, 2) collecting through (1, 2) and report the kept candidate.'),
            ('hard barrier without quorum supersedes soft barrier', {'keep': 1, 'indexes': tuple(range(9)), 'barriers': [
                {}, {'kind': 'hard', 'record': (3, 1), 'ingress': 0},
            ]},
                'When a confirmed soft barrier and an unconfirmed hard barrier both cover a kept blob,\n'
                'then default mode ignores the hard barrier and reports a soft-barrier candidate; write2x2 mode accepts the hard barrier and excludes the blob.'),
        ]
        results = [self._testcase(name, snapshot(**parameters), description)
                   for name, parameters, description in cases]
        return self._canonical_file('output.txt', '\n\n'.join(results) + '\n')


class TestScanReport(unittest.TestCase):
    def analyze(self, data, completed=None, check=True):
        completed = data.parameters['indexes'] if completed is None else completed
        binary = yatest.common.binary_path('ydb/tools/blobsan/blobsan')
        with tempfile.TemporaryDirectory() as directory:
            manifest, report = Path(directory) / 'manifest.json', Path(directory) / 'report.json'
            manifest.write_text(json.dumps(dict(group_id=1, completed_vdisks=list(completed))))
            command = [binary, '--write2x2-candidates', '--scan-manifest', str(manifest), '--scan-report', str(report)]
            command.append('mirror-3-dc')
            result = subprocess.run(command, input=bytes(data), stdout=subprocess.DEVNULL,
                                    stderr=subprocess.PIPE, check=check)
            return (json.loads(report.read_text()) if report.exists() else None), result

    def test_full_layouts_are_counted_in_group_realm_order(self):
        # Subgroup realms 0, 1, 2 map to group realms 2, 0, 1.
        cases = [({7: (0,), 1: (1,), 4: (2,)}, '1+1+1', 300),
                 ({7: (0,), 8: (0,), 1: (1,), 2: (1,)}, '2+0+2', 400)]
        for parts, local, replica_bytes in cases:
            with self.subTest(local=local):
                report, result = self.analyze(snapshot(indexes=range(9), local_realms=(), local_parts=parts,
                                                       known_nodes=(0, 1, 2)))
                self.assertNotIn(b'GroupId#', result.stderr)
                expected = dict(
                    local=local, ingress='1+1+1', gc='no_covering_barrier', blobs=1,
                    logical_bytes=100, observed_replica_bytes=replica_bytes, missing_metadata_blobs=0,
                    phantom_like=False, write2x2_candidate=False)
                self.assertEqual(report['buckets'], [expected])
                self.assertEqual(report['totals']['blobs'], 1)

    def test_missing_disk_makes_the_whole_realm_unknown(self):
        for completed, expected in [(tuple(i for i in range(9) if i != 6), '1+1+?'),
                                    (TWO_REALMS, '1+?+1'), ((7,), '?+?+?')]:
            with self.subTest(completed=completed):
                report, _ = self.analyze(snapshot(
                    indexes=completed, local_realms=(), known_nodes=(0, 1, 2), local_parts={7: (0,), 1: (1,), 4: (2,)}))
                self.assertEqual(report['buckets'][0]['local'], expected)
                self.assertEqual(report['buckets'][0]['ingress'], '1+1+1')
                self.assertEqual(report['completed_vdisks'], sorted(completed))

    def test_manifest_includes_empty_streams_and_missing_metadata_is_not_filtered(self):
        report, result = self.analyze(snapshot(indexes=(6, 7, 8)), completed=range(9))
        self.assertEqual(report['buckets'][0]['local'], '0+0+3')
        self.assertEqual(report['totals']['missing_metadata_blobs'], 1)
        self.assertNotIn(b'GroupId#', result.stderr)
        report, _ = self.analyze(snapshot(indexes=()), completed=range(9))
        self.assertEqual(report['buckets'], [])
        self.assertEqual(report['totals'], dict(blobs=0, logical_bytes=0, observed_replica_bytes=0, missing_metadata_blobs=0))

    def test_gc_categories_and_raw_local_parts_before_gc_filter(self):
        cases = [({'keep': 1, 'barrier': 'hard'}, 'hard_barrier'),
                 ({'keep': 2, 'barrier': 'hard'}, 'hard_barrier'),
                 ({'keep': 2}, 'do_not_keep'),
                 ({'barrier': 'soft'}, 'soft_without_keep'),
                 ({'keep': 1, 'barrier': 'soft'}, 'soft_with_keep'),
                 ({'keep': 1}, 'no_covering_barrier'),
                 ({}, 'no_covering_barrier')]
        for parameters, gc in cases:
            with self.subTest(parameters=parameters):
                report, _ = self.analyze(snapshot(indexes=range(9), **parameters))
                bucket = report['buckets'][0]
                self.assertEqual(bucket['gc'], gc)
                self.assertEqual(bucket['local'], '0+0+3')
                self.assertEqual(bucket['observed_replica_bytes'], 300)

    def test_duplicate_records_merge_and_distinct_blobs_accumulate(self):
        data = snapshot(indexes=range(9), local_realms=(0, 1, 2), keep_flags={7: (0, 1)})
        other = snapshot(indexes=range(9), local_realms=(0, 1, 2), blob_channel=5,
                         subgroup_order=CHANNEL5_SUBGROUP_ORDER)
        other_blobs = {index: blobs for index, _, blobs, _ in other.records}
        records = []
        for index, node, blobs, barriers in data.records:
            records.append((index, node, blobs + other_blobs[index], barriers))
        data.records = records
        report, _ = self.analyze(data)
        self.assertEqual(report['totals'], dict(blobs=2, logical_bytes=200, observed_replica_bytes=1800, missing_metadata_blobs=0))
        self.assertEqual(len(report['buckets']), 1)

    def test_wrong_realm_parts_do_not_count_as_replicas(self):
        report, _ = self.analyze(snapshot(indexes=range(9), local_realms=(), local_parts={7: (1,)}))
        self.assertEqual(report['buckets'][0]['local'], '0+0+0')
        self.assertEqual(report['totals']['observed_replica_bytes'], 0)

    def test_annotations_match_candidate_selection(self):
        cases = [
            dict(indexes=range(9)),
            dict(indexes=range(9), keep=1, barrier='soft'),
            dict(indexes=range(9), keep=1, barrier='hard'),
            dict(indexes=range(9), keep=2),
            dict(indexes=range(9), barrier='soft'),
            dict(indexes=range(9), local_realms=(0, 1, 2)),
            dict(indexes=range(9), local_realms=()),
            dict(indexes=range(9), local_realms=(), local_parts={7: (1,)}),
            dict(indexes=range(9), blob_indexes=(0, 1, 2, 6, 7, 8)),
            dict(indexes=TWO_REALMS),
            dict(indexes=(7,)),
            dict(indexes=range(9), known_full=False),
        ]
        for parameters in cases:
            with self.subTest(parameters=parameters):
                data = snapshot(**parameters)
                report, result = self.analyze(data)
                bucket, = report['buckets']
                self.assertEqual(bucket['write2x2_candidate'], b'GroupId#' in result.stderr)
                self.assertEqual(bucket['phantom_like'], bool(bucket['missing_metadata_blobs']))

    def test_same_layout_splits_phantom_and_candidate_annotations(self):
        data = snapshot(indexes=range(9), local_realms=(), local_parts={7: (0,), 1: (1,)},
                        known_nodes=(0, 1, 2))
        other = snapshot(indexes=range(9), local_realms=(), local_parts={0: (0,), 6: (2,)},
                         known_nodes=(0, 1, 2), blob_indexes=range(8), blob_channel=5,
                         subgroup_order=CHANNEL5_SUBGROUP_ORDER)
        other_blobs = {index: blobs for index, _, blobs, _ in other.records}
        data.records = [(index, node, blobs + other_blobs[index], barriers)
                        for index, node, blobs, barriers in data.records]
        report, result = self.analyze(data)
        self.assertEqual(len(report['buckets']), 2)
        self.assertEqual({(b['local'], b['ingress'], b['gc']) for b in report['buckets']},
                         {('1+0+1', '1+1+1', 'no_covering_barrier')})
        self.assertEqual({(b['phantom_like'], b['write2x2_candidate']) for b in report['buckets']},
                         {(False, True), (True, False)})
        self.assertEqual(result.stderr.count(b'GroupId#'), 1)

    def test_manifest_rejects_duplicate_and_unlisted_vdisks(self):
        for completed in [(7, 7), (9,), (-1,), (0, 1, 2)]:
            with self.subTest(completed=completed):
                report, result = self.analyze(snapshot(indexes=(7,)), completed=completed, check=False)
                self.assertNotEqual(result.returncode, 0)
                self.assertIsNone(report)

    def test_worker_to_pool_with_real_analyzer_and_partial_snapshot(self):
        from types import SimpleNamespace
        from unittest.mock import patch
        import threading
        import time
        from ydb.apps.dstool.lib import blobsan_service as svc

        binary = yatest.common.binary_path('ydb/tools/blobsan/blobsan')
        args = SimpleNamespace(viewer='http://viewer', request_timeout=5, scan_timeout=30, scan_only=True,
                               blobsan=binary, interval=3600, retry=60, refresh=300, lease=120)
        layout = dict(id=1, generation=7, fingerprint='v7', pool='pool1', pool_key='1:1',
                      slots=[dict(index=i, node=i + 1, pdisk=1, slot=100 + i) for i in range(9)])
        completed = set(TWO_REALMS)
        calls = {}

        def fetch(_args, url, raw=False):
            from urllib.parse import urlsplit
            index = int(urlsplit(url).path.split('/')[2]) - 1
            if index not in completed:
                raise OSError('DC unavailable')
            calls[index] = calls.get(index, 0) + 1
            if calls[index] > 1:
                return b''
            return bytes(snapshot(indexes=(index,), local_realms=(), known_nodes=(0, 1, 2),
                                  local_parts={7: (0,), 1: (1,)}))[12:]

        svc.STOP.clear()
        with tempfile.TemporaryDirectory() as directory:
            master = svc.Master(args, Path(directory))
            try:
                with master.db:
                    master.db.execute('INSERT INTO groups(id,layout,seen) VALUES(?,?,?)',
                                      (1, json.dumps(layout), time.time()))
                job = master.rpc('/claim', dict(worker='test'))['job']
                scratch = Path(directory) / 'scan'
                scratch.mkdir()
                with patch.object(svc, 'ydb_http', side_effect=fetch):
                    candidates, coverage = svc.scan(args, job, scratch, threading.Event())
                self.assertEqual(candidates, {'[1:1:1:0:0:100:0]'})
                reply = master.rpc('/result', dict(
                    worker='test', group=1, token=job['token'], result=dict(findings={}, findings_complete=False, **coverage)))
                self.assertTrue(reply['accepted'])
                pool, = json.loads(master.console('pools pool1'))
                self.assertEqual(pool['partial_groups'], 1)
                self.assertEqual(pool['totals']['logical_bytes'], 100)
                self.assertEqual(pool['buckets'][0]['local'], '1+?+1')
                detail = json.loads(master.console('layouts 1'))
                self.assertEqual(detail['missing_vdisks'], [3, 4, 5])
                self.assertEqual(detail['scan_report']['totals'], pool['totals'])
            finally:
                master.db.close()
