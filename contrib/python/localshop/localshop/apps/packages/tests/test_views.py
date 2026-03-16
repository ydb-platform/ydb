from base64 import standard_b64encode

import pytest
import requests

from localshop.apps.packages.models import Package
from localshop.apps.permissions.models import CIDR


@pytest.mark.parametrize('separator', ['\n', '\r\n'])
def test_package_upload(live_server, admin_user, separator):
    CIDR.objects.create(cidr='0.0.0.0/0', require_credentials=False)

    post_data = separator.join([
        '',
        '----------------GHSKFJDLGDS7543FJKLFHRE75642756743254',
        'Content-Disposition: form-data; name="comment"',
        '',
        '',
        '----------------GHSKFJDLGDS7543FJKLFHRE75642756743254',
        'Content-Disposition: form-data; name="metadata_version"',
        '',
        '1.0',
        '----------------GHSKFJDLGDS7543FJKLFHRE75642756743254',
        'Content-Disposition: form-data; name="filetype"',
        '',
        'sdist',
        '----------------GHSKFJDLGDS7543FJKLFHRE75642756743254',
        'Content-Disposition: form-data; name="protcol_version"',
        '',
        '1',
        '----------------GHSKFJDLGDS7543FJKLFHRE75642756743254',
        'Content-Disposition: form-data; name="author"',
        '',
        'Michael van Tellingen',
        '----------------GHSKFJDLGDS7543FJKLFHRE75642756743254',
        'Content-Disposition: form-data; name="home_page"',
        '',
        'http://github.com/mvantellingen/localshop',
        '----------------GHSKFJDLGDS7543FJKLFHRE75642756743254',
        'Content-Disposition: form-data; name="download_url"',
        '',
        'UNKNOWN',
        '----------------GHSKFJDLGDS7543FJKLFHRE75642756743254',
        'Content-Disposition: form-data; name="content";filename="tmpf3bcEV"',
        '',
        'binary-test-data-here',
        '----------------GHSKFJDLGDS7543FJKLFHRE75642756743254',
        'Content-Disposition: form-data; name="platform"',
        '',
        'UNKNOWN',
        '----------------GHSKFJDLGDS7543FJKLFHRE75642756743254',
        'Content-Disposition: form-data; name="version"',
        '',
        '0.1',
        '----------------GHSKFJDLGDS7543FJKLFHRE75642756743254',
        'Content-Disposition: form-data; name="description"',
        '',
        'UNKNOWN',
        '----------------GHSKFJDLGDS7543FJKLFHRE75642756743254',
        'Content-Disposition: form-data; name="md5_digest"',
        '',
        '06ffe94789d7bd9efba1109f40e935cf',
        '----------------GHSKFJDLGDS7543FJKLFHRE75642756743254',
        'Content-Disposition: form-data; name=":action"',
        '',
        'file_upload',
        '----------------GHSKFJDLGDS7543FJKLFHRE75642756743254',
        'Content-Disposition: form-data; name="name"',
        '',
        'localshop',
        '----------------GHSKFJDLGDS7543FJKLFHRE75642756743254',
        'Content-Disposition: form-data; name="license"',
        '',
        'BSD',
        '----------------GHSKFJDLGDS7543FJKLFHRE75642756743254',
        'Content-Disposition: form-data; name="pyversion"',
        '',
        'source',
        '----------------GHSKFJDLGDS7543FJKLFHRE75642756743254',
        'Content-Disposition: form-data; name="summary"',
        '',
        'A private pypi server including auto-mirroring of pypi.',
        '----------------GHSKFJDLGDS7543FJKLFHRE75642756743254',
        'Content-Disposition: form-data; name="author_email"',
        '',
        'michaelvantellingen@gmail.com',
        '----------------GHSKFJDLGDS7543FJKLFHRE75642756743254--',
        ''])

    headers = {
        'Content-type': 'multipart/form-data; boundary=--------------GHSKFJDLGDS7543FJKLFHRE75642756743254',
        'Authorization': 'Basic ' + standard_b64encode('admin:password')
    }

    response = requests.post(live_server + '/simple/', post_data, headers=headers)

    assert response.status_code == 200

    package = Package.objects.filter(name='localshop').first()

    assert package is not None
    assert package.is_local is True
    assert package.releases.count() == 1

    release = package.releases.first()

    assert release.author == 'Michael van Tellingen'
    assert release.author_email == 'michaelvantellingen@gmail.com'
    assert release.description == ''
    assert release.download_url == ''
    assert release.home_page == 'http://github.com/mvantellingen/localshop'
    assert release.license == 'BSD'
    assert release.metadata_version == '1.0'
    assert release.summary == 'A private pypi server including auto-mirroring of pypi.'
    assert release.user == admin_user
    assert release.version == '0.1'
    assert release.files.count() == 1

    release_file = release.files.first()

    assert release_file is not None
    assert release_file.python_version == 'source'
    assert release_file.filetype == 'sdist'
    assert release_file.md5_digest == '06ffe94789d7bd9efba1109f40e935cf'
    assert release_file.distribution.read() == 'binary-test-data-here'


def test_package_register(live_server, admin_user):
    CIDR.objects.create(cidr='0.0.0.0/0', require_credentials=False)

    post_data = '\n'.join([
        '',
        '----------------GHSKFJDLGDS7543FJKLFHRE75642756743254',
        'Content-Disposition: form-data; name="license"',
        '',
        'BSD',
        '----------------GHSKFJDLGDS7543FJKLFHRE75642756743254',
        'Content-Disposition: form-data; name="name"',
        '',
        'localshop',
        '----------------GHSKFJDLGDS7543FJKLFHRE75642756743254',
        'Content-Disposition: form-data; name="metadata_version"',
        '',
        '1.0',
        '----------------GHSKFJDLGDS7543FJKLFHRE75642756743254',
        'Content-Disposition: form-data; name="author"',
        '',
        'Michael van Tellingen',
        '----------------GHSKFJDLGDS7543FJKLFHRE75642756743254',
        'Content-Disposition: form-data; name="home_page"',
        '',
        'http://github.com/mvantellingen/localshop',
        '----------------GHSKFJDLGDS7543FJKLFHRE75642756743254',
        'Content-Disposition: form-data; name=":action"',
        '',
        'submit',
        '----------------GHSKFJDLGDS7543FJKLFHRE75642756743254',
        'Content-Disposition: form-data; name="download_url"',
        '',
        'UNKNOWN',
        '----------------GHSKFJDLGDS7543FJKLFHRE75642756743254',
        'Content-Disposition: form-data; name="summary"',
        '',
        'A private pypi server including auto-mirroring of pypi.',
        '----------------GHSKFJDLGDS7543FJKLFHRE75642756743254',
        'Content-Disposition: form-data; name="author_email"',
        '',
        'michaelvantellingen@gmail.com',
        '----------------GHSKFJDLGDS7543FJKLFHRE75642756743254',
        'Content-Disposition: form-data; name="version"',
        '',
        '0.1',
        '----------------GHSKFJDLGDS7543FJKLFHRE75642756743254',
        'Content-Disposition: form-data; name="platform"',
        '',
        'UNKNOWN',
        '----------------GHSKFJDLGDS7543FJKLFHRE75642756743254',
        'Content-Disposition: form-data; name="description"',
        '',
        'UNKNOWN',
        '----------------GHSKFJDLGDS7543FJKLFHRE75642756743254--',
        ''])

    headers = {
        'Content-type': 'multipart/form-data; boundary=--------------GHSKFJDLGDS7543FJKLFHRE75642756743254',
        'Authorization': 'Basic ' + standard_b64encode('admin:password')
    }

    response = requests.post(live_server + '/simple/', post_data, headers=headers)

    assert response.status_code == 200

    package = Package.objects.filter(name='localshop').first()

    assert package is not None
    assert package.is_local is True
    assert package.releases.count() == 1

    release = package.releases.first()

    assert release.author == 'Michael van Tellingen'
    assert release.author_email == 'michaelvantellingen@gmail.com'
    assert release.description == ''
    assert release.download_url == ''
    assert release.home_page == 'http://github.com/mvantellingen/localshop'
    assert release.license == 'BSD'
    assert release.metadata_version == '1.0'
    assert release.summary == 'A private pypi server including auto-mirroring of pypi.'
    assert release.user == admin_user
    assert release.version == '0.1'
