# -*- coding: utf-8 -*-
import sys

from crx3 import creator


def _generate_output_filename(source):
    # if the source ends in .zip, remove .zip and append .crx, if it does not end in .zip, just append .crx.
    if source.lower().endswith('.zip'):
        return source[:-4] + '.crx'
    return source + '.crx'


def _generate_private_key_filename(output_file):
    # remove .crx suffix from output_file, and append .pem
    return output_file[:-4] + '.pem'


def create(source, private_key_file='', output_file='', verbose=False):
    if output_file == '':
        output_file = _generate_output_filename(source)
    if private_key_file == '':
        private_key_file = _generate_private_key_filename(output_file)
        creator.create_private_key_file(private_key_file)
        if verbose:
            sys.stdout.write('private key file created at: {}\n'.format(private_key_file))
    creator.create_crx_file(source, private_key_file, output_file)
    if verbose:
        sys.stdout.write('crx file created at: {}\n'.format(output_file))
