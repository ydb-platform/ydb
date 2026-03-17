import collections
import logging
import os
import subprocess
import xml.etree.ElementTree

import dateutil.parser

import svn.constants
import svn.exception
import svn.common_base

_LOGGER = logging.getLogger(__name__)

_STATUS_ENTRY = \
    collections.namedtuple(
        '_STATUS_ENTRY', [
            'name',
            'type_raw_name',
            'type',
            'revision',
        ])

_FILE_HUNK_PREFIX = 'Index: '

_HUNK_HEADER_LEFT_PREFIX = '--- '
_HUNK_HEADER_RIGHT_PREFIX = '+++ '
_HUNK_HEADER_LINE_NUMBERS_PREFIX = '@@ '


class CommonClient(svn.common_base.CommonBase):
    def __init__(self, url_or_path, type_, username=None, password=None,
                 svn_filepath='svn', trust_cert=None, env={}, *args, **kwargs):
        super(CommonClient, self).__init__(*args, **kwargs)

        self.__url_or_path = url_or_path
        self.__username = username
        self.__password = password
        self.__svn_filepath = svn_filepath
        self.__trust_cert = trust_cert
        self.__env = env

        if type_ not in (svn.constants.LT_URL, svn.constants.LT_PATH):
            raise svn.exception.SvnException("Type is invalid: {}".format(type_))

        self.__type = type_

    def run_command(self, subcommand, args, **kwargs):
        cmd = [self.__svn_filepath, '--non-interactive']

        if self.__trust_cert:
            cmd += ['--trust-server-cert']

        if self.__username is not None and self.__password is not None:
            cmd += ['--username', self.__username]
            cmd += ['--password', self.__password]
            cmd += ['--no-auth-cache']

        cmd += [subcommand] + args
        return self.external_command(cmd, environment=self.__env, **kwargs)

    def __element_text(self, element):
        """Return ElementTree text or None
        :param xml.etree.ElementTree element: ElementTree to get text.

        :return str|None: Element text
        """
        if element is not None and len(element.text):
            return element.text

        return None

    def info(self, rel_path=None, revision=None):
        cmd = []
        if revision is not None:
            cmd += ['-r', str(revision)]

        full_url_or_path = self.__url_or_path
        if rel_path is not None:
            full_url_or_path += '/' + rel_path
        cmd += ['--xml', full_url_or_path]

        result = self.run_command(
            'info',
            cmd,
            do_combine=True)

        root = xml.etree.ElementTree.fromstring(result)

        entry_attr = root.find('entry').attrib
        commit_attr = root.find('entry/commit').attrib

        relative_url = root.find('entry/relative-url')
        author = root.find('entry/commit/author')
        wcroot_abspath = root.find('entry/wc-info/wcroot-abspath')
        wcinfo_schedule = root.find('entry/wc-info/schedule')
        wcinfo_depth = root.find('entry/wc-info/depth')

        info = {
            'url': root.find('entry/url').text,

            'relative_url': self.__element_text(relative_url),

# TODO(dustin): These are just for backwards-compatibility. Use the ones added
#               below.

            'entry#kind': entry_attr['kind'],
            'entry#path': entry_attr['path'],
            'entry#revision': int(entry_attr['revision']),

            'repository/root': root.find('entry/repository/root').text,
            'repository/uuid': root.find('entry/repository/uuid').text,

            'wc-info/wcroot-abspath': self.__element_text(wcroot_abspath),
            'wc-info/schedule': self.__element_text(wcinfo_schedule),
            'wc-info/depth': self.__element_text(wcinfo_depth),
            'commit/author': self.__element_text(author),

            'commit/date': dateutil.parser.parse(
                root.find('entry/commit/date').text),
            'commit#revision': int(commit_attr['revision']),
        }

        # Set some more intuitive keys, because no one likes dealing with
        # symbols. However, we retain the old ones to maintain backwards-
        # compatibility.

# TODO(dustin): Should we be casting the integers?
# TODO(dustin): Convert to namedtuple in the next version.

        info['entry_kind'] = info['entry#kind']
        info['entry_path'] = info['entry#path']
        info['entry_revision'] = info['entry#revision']
        info['repository_root'] = info['repository/root']
        info['repository_uuid'] = info['repository/uuid']
        info['wcinfo_wcroot_abspath'] = info['wc-info/wcroot-abspath']
        info['wcinfo_schedule'] = info['wc-info/schedule']
        info['wcinfo_depth'] = info['wc-info/depth']
        info['commit_author'] = info['commit/author']
        info['commit_date'] = info['commit/date']
        info['commit_revision'] = info['commit#revision']

        return info

    def properties(self, rel_path=None):
        """ Return a dictionary with all svn-properties associated with a
            relative path.
        :param rel_path: relative path in the svn repo to query the
                         properties from
        :returns: a dictionary with the property name as key and the content
                  as value
        """

        full_url_or_path = self.__url_or_path
        if rel_path is not None:
            full_url_or_path += '/' + rel_path

        result = self.run_command(
            'proplist',
            ['--xml', full_url_or_path],
            do_combine=True)

        # query the proper list of this path
        root = xml.etree.ElementTree.fromstring(result)
        target_elem = root.find('target')
        property_names = [p.attrib["name"]
                          for p in target_elem.findall('property')]

        # now query the content of each propery
        property_dict = {}

        for property_name in property_names:
            result = self.run_command(
                'propget',
                ['--xml', property_name, full_url_or_path, ],
                do_combine=True)
            root = xml.etree.ElementTree.fromstring(result)
            target_elem = root.find('target')
            property_elem = target_elem.find('property')
            property_dict[property_name] = property_elem.text

        return property_dict

    def cat(self, rel_filepath, revision=None):
        cmd = []
        if revision is not None:
            cmd += ['-r', str(revision)]
        cmd += [self.__url_or_path + '/' + rel_filepath]
        return self.run_command('cat', cmd, return_binary=True)

    def log_default(self, timestamp_from_dt=None, timestamp_to_dt=None,
                    limit=None, rel_filepath=None, stop_on_copy=False,
                    revision_from=None, revision_to=None, changelist=False,
                    use_merge_history=False):
        """Allow for the most-likely kind of log listing: the complete list,
        a FROM and TO timestamp, a FROM timestamp only, or a quantity limit.
        """

        full_url_or_path = self.__url_or_path
        if rel_filepath is not None:
            full_url_or_path += '/' + rel_filepath

        timestamp_from_phrase = ('{' + timestamp_from_dt.isoformat() + '}') \
            if timestamp_from_dt \
            else ''

        timestamp_to_phrase = ('{' + timestamp_to_dt.isoformat() + '}') \
            if timestamp_to_dt \
            else ''

        args = []

        if timestamp_from_phrase or timestamp_to_phrase:
            if not timestamp_from_phrase:
                raise ValueError("The default log retriever can not take a TO "
                                 "timestamp without a FROM timestamp.")

            if not timestamp_to_phrase:
                timestamp_to_phrase = 'HEAD'

            args += ['-r', timestamp_from_phrase + ':' + timestamp_to_phrase]

        if revision_from or revision_to:
            if timestamp_from_phrase or timestamp_to_phrase:
                raise ValueError("The default log retriever can not take both "
                                 "timestamp and revision number ranges.")

            if not revision_from:
                revision_from = '1'

            if not revision_to:
                revision_to = 'HEAD'

            args += ['-r', str(revision_from) + ':' + str(revision_to)]

        if limit is not None:
            args += ['-l', str(limit)]

        if stop_on_copy is True:
            args += ['--stop-on-copy']

        if use_merge_history is True:
            args += ['--use-merge-history']

        if changelist is True:
            args += ['--verbose']

        result = self.run_command(
            'log',
            args + ['--xml', full_url_or_path],
            do_combine=True)

        root = xml.etree.ElementTree.fromstring(result)
        named_fields = ['date', 'msg', 'revision', 'author', 'changelist']
        c = collections.namedtuple(
            'LogEntry',
            named_fields)

        # Merge history can create nested log entries, so use iter instead of findall
        for e in root.iter('logentry'):
            entry_info = {x.tag: x.text for x in e}

            date = None
            date_text = entry_info.get('date')
            if date_text is not None:
                date = dateutil.parser.parse(date_text)

            log_entry = {
                'msg': entry_info.get('msg'),
                'author': entry_info.get('author'),
                'revision': int(e.get('revision')),
                'date': date
            }

            if changelist is True:
                cl = []
                for ch in e.findall('paths/path'):
                    cl.append((ch.attrib['action'], ch.text))

                log_entry['changelist'] = cl
            else:
                log_entry['changelist'] = None

            yield c(**log_entry)

    def export(self, to_path, revision=None, force=False):
        cmd = []

        if revision is not None:
            cmd += ['-r', str(revision)]

        cmd += [self.__url_or_path, to_path]
        cmd.append('--force') if force else None

        self.run_command('export', cmd)

    def status(self, rel_path=None):
        full_url_or_path = self.__url_or_path
        if rel_path is not None:
            full_url_or_path += '/' + rel_path

        raw = self.run_command(
            'status',
            ['--xml', full_url_or_path],
            do_combine=True)

        root = xml.etree.ElementTree.fromstring(raw)

        list_ = root.findall('target/entry')
        for entry in list_:
            entry_attr = entry.attrib
            name = entry_attr['path']

            wcstatus = entry.find('wc-status')
            wcstatus_attr = wcstatus.attrib

            change_type_raw = wcstatus_attr['item']
            change_type = svn.constants.STATUS_TYPE_LOOKUP[change_type_raw]

            # This will be absent if the file is "unversioned". It'll be "-1"
            # if added but not committed.
            revision = wcstatus_attr.get('revision')
            if revision is not None:
                revision = int(revision)

            yield _STATUS_ENTRY(
                name=name,
                type_raw_name=change_type_raw,
                type=change_type,
                revision=revision
            )

    def list(self, extended=False, rel_path=None):
        full_url_or_path = self.__url_or_path
        if rel_path is not None:
            full_url_or_path += '/' + rel_path

        if extended is False:
            for line in self.run_command(
                    'ls',
                    [full_url_or_path]):
                line = line.strip()
                if line:
                    yield line

        else:
            raw = self.run_command(
                'ls',
                ['--xml', full_url_or_path],
                do_combine=True)

            root = xml.etree.ElementTree.fromstring(raw)

            list_ = root.findall('list/entry')
            for entry in list_:
                entry_attr = entry.attrib

                kind = entry_attr['kind']
                name = entry.find('name').text

                size = entry.find('size')

                # This will be None for directories.
                if size is not None:
                    size = int(size.text)

                commit_node = entry.find('commit')

                author = commit_node.find('author').text
                date = dateutil.parser.parse(commit_node.find('date').text)

                commit_attr = commit_node.attrib
                revision = int(commit_attr['revision'])

# TODO(dustin): Convert this to a namedtuple in the next version.
                entry = {
                    'kind': kind,

                    # To decouple people from the knowledge of the value.
                    'is_directory': kind == svn.constants.K_DIR,

                    'name': name,
                    'size': size,
                    'author': author,
                    'date': date,

                    # Our approach to normalizing a goofy field-name.
                    'timestamp': date,

                    'commit_revision': revision,
                }

                yield entry

    def list_recursive(self, rel_path=None, yield_dirs=False,
                       path_filter_cb=None):
        q = [rel_path]
        while q:
            current_rel_path = q[0]
            del q[0]

            for entry in self.list(extended=True, rel_path=current_rel_path):
                if entry['is_directory'] is True:
                    if current_rel_path is not None:
                        next_rel_path = \
                            os.path.join(current_rel_path, entry['name'])
                    else:
                        next_rel_path = entry['name']

                    do_queue = True
                    if path_filter_cb is not None:
                        result = path_filter_cb(next_rel_path)
                        if result is False:
                            do_queue = False

                    if do_queue is True:
                        q.append(next_rel_path)

                if entry['is_directory'] is False or yield_dirs is True:
                    current_rel_path_phrase = current_rel_path \
                        if current_rel_path is not None \
                        else ''

                    yield (current_rel_path_phrase, entry)

    def diff_summary(self, old, new, rel_path=None):
        """Provides a summarized output of a diff between two revisions
        (file, change type, file type)
        """

        full_url_or_path = self.__url_or_path
        if rel_path is not None:
            full_url_or_path += '/' + rel_path

        arguments = [
            '--old', '{0}@{1}'.format(full_url_or_path, old),
            '--new', '{0}@{1}'.format(full_url_or_path, new),
            '--summarize',
            '--xml',
        ]

        result = self.run_command(
            'diff',
            arguments,
            do_combine=True)

        root = xml.etree.ElementTree.fromstring(result)

        diff = []
        for element in root.findall('paths/path'):
            diff.append({
                'path': element.text,
                'item': element.attrib['item'],
                'kind': element.attrib['kind'],
            })

        return diff

    def diff(self, old, new, rel_path=None):
        """Provides output of a diff between two revisions (file, change type,
        file type)
        """

        full_url_or_path = self.__url_or_path
        if rel_path is not None:
            full_url_or_path += '/' + rel_path

        arguments = [
            '--old', '{0}@{1}'.format(full_url_or_path, old),
            '--new', '{0}@{1}'.format(full_url_or_path, new),
        ]

        diff_result = \
            self.run_command(
            'diff', arguments,
            do_combine=True)

        diff_result = diff_result.strip()

        # Split the hunks.

        # Index: /tmp/testsvnwc/bb
        # ===================================================================
        # --- /tmp/testsvnwc/bb   (nonexistent)
        # +++ /tmp/testsvnwc/bb   (revision 3)
        # @@ -0,0 +1 @@
        # +Sat Feb  1 03:14:10 EST 2020
        # Index: /tmp/testsvnwc/cc
        # ===================================================================
        # --- /tmp/testsvnwc/cc   (nonexistent)
        # +++ /tmp/testsvnwc/cc   (revision 3)
        # @@ -0,0 +1 @@
        # +Sat Feb  1 03:14:27 EST 2020

        hunks = {}

        def _process_hunk(file_hunk_raw):
            filepath, hunks_info = self._split_file_hunk(file_hunk_raw)
            hunks[filepath] = hunks_info

        while True:
            if not diff_result:
                break

            assert \
                diff_result.startswith(_FILE_HUNK_PREFIX), \
                "Diff output doesn't start with 'Index:':\n{}".format(
                diff_result)

            try:
                next_index = diff_result.index(_FILE_HUNK_PREFIX, 1)
            except ValueError:
                _process_hunk(diff_result)
                break

            file_hunk_raw, diff_result = diff_result[:next_index], diff_result[next_index:]

            _process_hunk(file_hunk_raw)

        return hunks

    def _split_file_hunk(self, file_hunk):
        # Parse the filename out of the header and drop the header from the
        # hunk.

        lines = file_hunk.split('\n')

        # Index: /tmp/testsvnwc/bb
        # ===================================================================
        filepath = lines[0][len(_FILE_HUNK_PREFIX):]

        assert \
            lines[2].startswith(_HUNK_HEADER_LEFT_PREFIX), \
            "Could not find 'left' header prefix: [{}]".format(lines[2])

        assert \
            lines[3].startswith(_HUNK_HEADER_RIGHT_PREFIX), \
            "Could not find 'right' header prefix: [{}]".format(lines[3])

        # --- /tmp/testsvnwc/cc   (revision 5)
        # +++ /tmp/testsvnwc/cc   (revision 6)
        # @@ -33,6 +33,7 @@
        #  nova/sdk/certgen   developertools/certgen  master
        #  nova/apps/search   nova/apps/search    develop
        #  external/conscrypt nvidia/android/platform/external/conscrypt  ml-t186-n-dev
        # +testline1
        #  vendor/nvidia/tegra/adsp/adsp-t21x nvidia/tegra/adsp/adsp-t21x ml/rel-28r10
        #  device/generic/armv7-a-neon    nvidia/android/device/generic/armv7-a-neon  ml-t186-n-dev
        #  robot_test_apps/unity_robot_test   ml/unity_robot_test master
        # @@ -236,6 +237,7 @@
        #  hardware/nvidia/soc/t18x   nvidia/device/hardware/nvidia/soc/t18x  ml-t186-n-dev
        #  vendor/nvidia/tegra/multimedia nvidia/tegra/prebuilts-multimedia-headers-standard  ml-t186-n-dev
        #  external/c-ares    nvidia/android/platform/external/c-ares ml-t186-n-dev
        # +testline2
        #  external/chromium-webview  nvidia/android/platform/external/chromium-webview   ml-t186-n-dev
        #  nova/apps/landscapemanager nova/apps/landscapemanager  master
        #  external/bzip2 nvidia/android/platform/external/bzip2  ml-t186-n-dev

        file_hunk_left_phrase = lines[2][len(_HUNK_HEADER_LEFT_PREFIX):].split('\t')
        file_hunk_right_phrase = lines[3][len(_HUNK_HEADER_RIGHT_PREFIX):].split('\t')

        lines = lines[4:]

        hunks = []
        while True:
            if not lines:
                break

            # @@ -33,6 +33,7 @@
            #  nova/sdk/certgen   developertools/certgen  master
            #  nova/apps/search   nova/apps/search    develop
            #  external/conscrypt nvidia/android/platform/external/conscrypt  ml-t186-n-dev
            # +testline1
            #  vendor/nvidia/tegra/adsp/adsp-t21x nvidia/tegra/adsp/adsp-t21x ml/rel-28r10
            #  device/generic/armv7-a-neon    nvidia/android/device/generic/armv7-a-neon  ml-t186-n-dev
            #  robot_test_apps/unity_robot_test   ml/unity_robot_test master

            hunk_lines_phrase = lines[0]
            lines = lines[1:]

            hunk_lines = []
            for line in lines:
                # We've run into the next hunk.
                if line.startswith(_HUNK_HEADER_LINE_NUMBERS_PREFIX) is True:
                    break

                hunk_lines.append(line)

            lines = lines[len(hunk_lines):]

            hunks.append({
                'lines_phrase': hunk_lines_phrase,
                'body': '\n'.join(hunk_lines),
            })

        hunks_info = {
            'left_phrase': file_hunk_left_phrase,
            'right_phrase': file_hunk_right_phrase,
            'hunks': hunks,
        }

        return filepath, hunks_info

    @property
    def url(self):
        if self.__type != svn.constants.LT_URL:
            raise EnvironmentError(
                "Only the remote-client has access to the URL.")

        return self.__url_or_path

    @property
    def path(self):
        if self.__type != svn.constants.LT_PATH:
            raise EnvironmentError(
                "Only the local-client has access to the path.")

        return self.__url_or_path
