# Copyright 2021 Appknox <engineering@appknox.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os

import click

from pyaxmlparser import APK

import logging

@click.command()
@click.argument('filename')
@click.option('--silent', '-s', default=False, is_flag=True, help="Don't print any debug or warning logs")
def main(filename, silent):
    if silent:
        logging.basicConfig(level=logging.ERROR)

    filename = os.path.expanduser(filename)
    apk = APK(filename)

    click.echo('APK: {}'.format(filename))
    click.echo('App name: {}'.format(apk.application))
    click.echo('Package: {}'.format(apk.packagename))
    click.echo('Version name: {}'.format(apk.version_name))
    click.echo('Version code: {}'.format(apk.version_code))
    click.echo('Is it Signed: {}'.format(apk.signed))
    click.echo('Is it Signed with v1 Signatures: {}'.format(apk.signed_v1))
    click.echo('Is it Signed with v2 Signatures: {}'.format(apk.signed_v2))
    click.echo('Is it Signed with v3 Signatures: {}'.format(apk.signed_v3))
