# TODO: various stuff just pasted together in one place; needs clean up

# -*- coding: utf-8 -*-

from . import __version__
from .report import report

import os
import importlib
import inspect
import argparse
import shutil

def _find_examples(name):
    module_path = os.path.dirname(inspect.getfile(importlib.import_module(name)))
    candidates = [
        # installed package
        os.path.join(module_path,"examples"),
        # git repo
        os.path.join(module_path,"..","examples")]

    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate

    raise ValueError("Could not find examples for %s at any of %s"%(name,candidates))

def examples(name,path,verbose=False,use_test_data=False,force=False):
    """
    Copy examples and fetch data (if any) to the supplied path.
    See copy-examples and fetch-data for more flexibility.

    NOTE: force operates both on example and data over-writing
    pre-existing files.
    """
    copy_examples(name, path, verbose, force)
    fetch_data(name,path,require_datasets=False,use_test_data=use_test_data,force=force)


def copy_examples(name,path,verbose=False,force=False):
    """Copy examples to the supplied path."""
    source = _find_examples(name)
    path = os.path.abspath(path)
    if os.path.exists(path) and not force:
        raise ValueError("Path %s already exists; please move it away, choose a different path, or use force."%path)
    if verbose:
        print("Copying examples from %s"%source)
    shutil.copytree(source, path, dirs_exist_ok=True)
    print("Copied examples to %s"%path)

"""
Copyright (c) 2011, Kenneth Reitz <me@kennethreitz.com>

Permission to use, copy, modify, and/or distribute this software for any
purpose with or without fee is hereby granted, provided that the above
copyright notice and this permission notice appear in all copies.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

clint.textui.progress
~~~~~~~~~~~~~~~~~

This module provides the progressbar functionality.

"""
from collections import OrderedDict  # noqa: E402
import glob  # noqa: E402
import sys  # noqa: E402
import tarfile  # noqa: E402
import time  # noqa: E402
import zipfile  # noqa: E402
import yaml  # noqa: E402

try:
    import requests  # noqa: E402
except ImportError:
    requests = None

# TODO
#    if requests is None:
#        print('this download script requires the requests module: conda install requests')
#        sys.exit(1)


STREAM = sys.stderr

BAR_TEMPLATE = '%s[%s%s] %i/%i - %s\r'
MILL_TEMPLATE = '%s %s %i/%i\r'

DOTS_CHAR = '.'
BAR_FILLED_CHAR = '#'
BAR_EMPTY_CHAR = ' '
MILL_CHARS = ['|', '/', '-', '\\']

# How long to wait before recalculating the ETA
ETA_INTERVAL = 1
# How many intervals (excluding the current one) to calculate the simple moving
# average
ETA_SMA_WINDOW = 9
DATA_DIR = 'data'
DATA_STUBS_DIR = '.data_stubs'


class Bar(object):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.done()
        return False  # we're not suppressing exceptions

    def __init__(self, label='', width=32, hide=None, empty_char=BAR_EMPTY_CHAR,
                 filled_char=BAR_FILLED_CHAR, expected_size=None, every=1):
        '''Bar is a class for printing the status of downloads'''
        self.label = label
        self.width = width
        self.hide = hide
        # Only show bar in terminals by default (better for piping, logging etc.)
        if hide is None:
            try:
                self.hide = not STREAM.isatty()
            except AttributeError:  # output does not support isatty()
                self.hide = True
        self.empty_char =    empty_char
        self.filled_char =   filled_char
        self.expected_size = expected_size
        self.every =         every
        self.start =         time.time()
        self.ittimes =       []
        self.eta =           0
        self.etadelta =      time.time()
        self.etadisp =       self.format_time(self.eta)
        self.last_progress = 0
        if (self.expected_size):
            self.show(0)

    def show(self, progress, count=None):
        if count is not None:
            self.expected_size = count
        if self.expected_size is None:
            raise Exception("expected_size not initialized")
        self.last_progress = progress
        if (time.time() - self.etadelta) > ETA_INTERVAL:
            self.etadelta = time.time()
            self.ittimes = \
                self.ittimes[-ETA_SMA_WINDOW:] + \
                    [-(self.start - time.time()) / (progress+1)]
            self.eta = \
                sum(self.ittimes) / float(len(self.ittimes)) * \
                (self.expected_size - progress)
            self.etadisp = self.format_time(self.eta)
        x = int(self.width * progress / self.expected_size)
        if not self.hide:
            if ((progress % self.every) == 0 or      # True every "every" updates
                (progress == self.expected_size)):   # And when we're done
                STREAM.write(BAR_TEMPLATE % (
                    self.label, self.filled_char * x,
                    self.empty_char * (self.width - x), progress,
                    self.expected_size, self.etadisp))
                STREAM.flush()

    def done(self):
        self.elapsed = time.time() - self.start
        elapsed_disp = self.format_time(self.elapsed)
        if not self.hide:
            # Print completed bar with elapsed time
            STREAM.write(BAR_TEMPLATE % (
                self.label, self.filled_char * self.width,
                self.empty_char * 0, self.last_progress,
                self.expected_size, elapsed_disp))
            STREAM.write('\n')
            STREAM.flush()

    def format_time(self, seconds):
        return time.strftime('%H:%M:%S', time.gmtime(seconds))


def bar(it, label='', width=32, hide=None, empty_char=BAR_EMPTY_CHAR,
        filled_char=BAR_FILLED_CHAR, expected_size=None, every=1):
    """Progress iterator. Wrap your iterables with it."""

    count = len(it) if expected_size is None else expected_size

    with Bar(label=label, width=width, hide=hide, empty_char=BAR_EMPTY_CHAR,
             filled_char=BAR_FILLED_CHAR, expected_size=count, every=every) \
            as bar:
        for i, item in enumerate(it):
            yield item
            bar.show(i + 1)


def ordered_load(stream, Loader=yaml.Loader, object_pairs_hook=OrderedDict):
    class OrderedLoader(Loader):
        pass
    def construct_mapping(loader, node):
        loader.flatten_mapping(node)
        return object_pairs_hook(loader.construct_pairs(node))
    OrderedLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
        construct_mapping)
    return yaml.load(stream, OrderedLoader)


class DirectoryContext(object):
    """
    Context Manager for changing directories
    """
    def __init__(self, path):
        self.old_dir = os.getcwd()
        self.new_dir = path

    def __enter__(self):
        os.chdir(self.new_dir)

    def __exit__(self, *args):
        os.chdir(self.old_dir)


def _url_to_binary_write(url, output_path, title):
    '''Given a url, output_path and title,
    write the contents of a requests get operation to
    the url in binary mode and print the title of operation'''
    print('Downloading {0}'.format(title))
    resp = requests.get(url, stream=True)
    try:
        with open(output_path, 'wb') as f:
            total_length = int(resp.headers.get('content-length'))
            for chunk in bar(resp.iter_content(chunk_size=1024), expected_size=(total_length/1024) + 1, every=1000):
                if chunk:
                    f.write(chunk)
                    f.flush()
    except:
        # Don't leave a half-written zip file
        if os.path.exists(output_path):
            os.remove(output_path)
        raise


def _extract_downloaded_archive(output_path):
    '''Extract a local archive, e.g. zip or tar, then
    delete the archive'''
    if output_path.endswith("tar.gz"):
        with tarfile.open(output_path, "r:gz") as tar:
            tar.extractall()
        os.remove(output_path)
    elif output_path.endswith("tar"):
        with tarfile.open(output_path, "r:") as tar:
            tar.extractall()
        os.remove(output_path)
    elif output_path.endswith("tar.bz2"):
        with tarfile.open(output_path, "r:bz2") as tar:
            tar.extractall()
        os.remove(output_path)
    elif output_path.endswith("zip"):
        with zipfile.ZipFile(output_path, 'r') as zipf:
            zipf.extractall()
        os.remove(output_path)


def _process_dataset(dataset, output_dir, here, use_test_data=False, force=False):
    '''Process each download spec in datasets.yml

    Typically each dataset list entry in the yml has
    "files" and "url" and "title" keys/values to show
    local files that must be present / extracted from
    a decompression of contents downloaded from the url.

    If a url endswith '/', then all files given
    are assumed to be added to the url pattern at the
    end
    '''
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with DirectoryContext(output_dir):
        requires_download = False
        for f in dataset.get('files', []):
            if not os.path.exists(f):
                requires_download = True
                break

        if force is False and not requires_download:
            print('Skipping {0}'.format(dataset['title']))
            return
        url = dataset['url']
        title_fmt = dataset['title'] + ' {} of {}'
        if url.endswith('/'):
            urls = [url + f for f in dataset['files']]
            output_paths = [os.path.join(here, DATA_DIR, fname)
                            for fname in dataset['files']]

            unpacked = ['.'.join(output_path.split('.')[:(-2 if output_path.endswith('gz') else -1)]) + '*'
                        for output_path in output_paths]
        else:
            urls = [url]
            output_paths = [os.path.split(url)[1]]
            unpacked = dataset['files']
            if not isinstance(unpacked, (tuple, list)):
                unpacked = [unpacked]
        zipped = zip(urls, output_paths, unpacked)
        for idx, (url, output_path, unpack) in enumerate(zipped):
            running_title = title_fmt.format(idx + 1, len(urls))
            if force is False and (glob.glob(unpack) or os.path.exists(unpack.replace('*',''))):
                # Skip a file if a similar one is downloaded:
                # i.e. one that has same name but dif't extension
                print('Skipping {0}'.format(running_title))
                continue
            test = os.path.join(output_dir, DATA_STUBS_DIR, unpack)
            if use_test_data and os.path.exists(test):
                target = os.path.join(output_dir, unpack)
                print("Copying test data file '{0}' to '{1}'".format(test, target))
                shutil.copyfile(test, target)
                continue
            elif use_test_data and not os.path.exists(test):
                print("No test file found for: {}. Using regular file instead".format(test))
            _url_to_binary_write(url, output_path, running_title)
            _extract_downloaded_archive(output_path)


    if requests is None:
        print('this download script requires the requests module: conda install requests')
        sys.exit(1)

def fetch_data(name,path,datasets="datasets.yml",require_datasets=True,use_test_data=False,force=False):
    '''Fetch sample datasets as defined by path/datasets if it exists or else module's own examples/datasets otherwise.

    Datasets are placed in path/data
    '''
    path = os.path.abspath(path)
    info_file = os.path.join(path,datasets)
    if not os.path.exists(info_file):
        info_file = os.path.join(_find_examples(name),datasets)

    if not os.path.exists(info_file) and require_datasets is False:
        print("No datasets to download")
        return

    print("Fetching data defined in %s and placing in %s"%(info_file,os.path.join(path,DATA_DIR))) # data is added later...

    with open(info_file) as f:
        info = ordered_load(f.read())
        for topic, downloads in info.items():
            output_dir = os.path.join(path, topic)
            for d in downloads:
                _process_dataset(d, output_dir, path, use_test_data=use_test_data, force=force)

def clean_data(name, path):
    '''Remove up any data files that are copied from test files
    '''
    path = os.path.abspath(path)
    if not os.path.exists(path):
        path = _find_examples(name)

    data_dir = os.path.join(path, DATA_DIR)
    test_dir = os.path.join(data_dir, DATA_STUBS_DIR)
    if not os.path.exists(test_dir) or len(os.listdir(test_dir)) == 0:
        print("No test files found")
        return

    for f in os.listdir(test_dir):
        data_file = os.path.join(data_dir, f)
        if not os.path.isfile(data_file):
            print("Test file was not copied to data:", f)
            continue

        test_file = os.path.join(test_dir, f)
        if os.path.isfile(test_file):
            data_s = os.path.getsize(data_file)
            test_s = os.path.getsize(test_file)
            if data_s == test_s:
                print("Removing copied test file:", f)
                os.remove(data_file)
            else:
                print("Size of test file {:.2e} did not match "
                      "size of data file {:.2e}".format(test_s, data_s))

def _add_common_args(parser, name, *args):
    if '-v' in args:
        parser.add_argument('-v', '--verbose', action='count', default=0)
    if '--path' in args:
        parser.add_argument('--path',type=str, help='where to place output', default='{}-examples'.format(name))


def _set_defaults(parser, name, fn):
    parser.set_defaults(func=lambda args: fn(name, **{k: getattr(args,k) for k in vars(args) if k!='func'} ))

# TODO: cmds=None defaults to 'all', basically, which is a bit confusing
def add_commands(parser, name, cmds=None, args=None):
    """
    Add all commands in pyct.cmd unless specific cmds are listed
    """
    if cmds is None:
        cmds = ['examples','copy-examples','fetch-data','clean-data']

    # use dict/reg instead
    if 'copy-examples' in cmds:
        eg_parser = parser.add_parser('copy-examples', help=inspect.getdoc(copy_examples))
        eg_parser.add_argument('--force', action='store_true',
                               help=('if PATH already exists, force overwrite existing '
                                     'files if older than source files'))
        _add_common_args(eg_parser, name, '-v', '--path')
        _set_defaults(eg_parser, name, copy_examples)

    if 'fetch-data' in cmds:
        d_parser = parser.add_parser('fetch-data', help=inspect.getdoc(fetch_data))
        d_parser.add_argument('--datasets', type=str, default='datasets.yml',
                              help=('*name* of datasets file; must exist either in path '
                                    'specified by --path or in package/examples/'))
        d_parser.add_argument('--force', action='store_true', help='Force any existing data files to be replaced')
        d_parser.add_argument('--use-test-data', action='store_true',
                              help=("Use data's test files, if any, instead of fetching full data. "
                                    "If test file not in '.data_stubs', fall back to fetching full data."))
        _add_common_args(d_parser, name, '--path')
        _set_defaults(d_parser, name, fetch_data)

    if 'examples' in cmds:
        egd_parser = parser.add_parser('examples', help=inspect.getdoc(examples))
        egd_parser.add_argument('--force', action='store_true',
                                help=('if PATH already exists, force overwrite existing examples if older '
                                      'than source examples. ALSO force any existing data files to be replaced'))
        egd_parser.add_argument('--use-test-data', action='store_true',
                                help=("Use data's test files, if any, instead of fetching full data. "
                                      "If test file not in '.data_stubs', fall back to fetching full data."))
        _add_common_args(egd_parser, name, '-v', '--path')
        _set_defaults(egd_parser, name, examples)

    if 'clean-data' in cmds:
        cd_parser = parser.add_parser('clean-data', help=inspect.getdoc(clean_data))
        _add_common_args(cd_parser, name, '--path')
        _set_defaults(cd_parser, name, clean_data)

def add_version(parser, name):
    mod = importlib.import_module(name)
    parser.add_argument('--version', action='version', version='%(prog)s ' + mod.__version__)

def substitute_main(name, cmds=None, args=None):
    """
    If module has no other commands, use this function to add all of the ones in pyct.cmd
    """
    parser = argparse.ArgumentParser(description="%s commands"%name)
    subparsers = parser.add_subparsers(title='available commands')
    add_commands(subparsers, name, cmds, args)
    add_version(parser, name)

    args = parser.parse_args()
    args.func(args) if hasattr(args,'func') else parser.error("must supply command to run")

def main():
    parser = argparse.ArgumentParser(description="Commands relating to versioning")
    parser.add_argument('--version', action='version', version='%(prog)s '+__version__)

    subparsers = parser.add_subparsers(title='available commands')

    report_parser = subparsers.add_parser('report', help=inspect.getdoc(report))
    report_parser.set_defaults(func=report)
    report_parser.add_argument('packages',metavar='package',type=str,nargs='+',
                               help='name of package')

    args = parser.parse_args()

    if hasattr(args,'func'):
        args.func(*args.packages)
    else:
        parser.error("must supply command to run")
