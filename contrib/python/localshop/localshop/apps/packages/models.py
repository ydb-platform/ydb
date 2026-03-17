import logging
import os
from shutil import copyfileobj
from tempfile import NamedTemporaryFile

import docutils.core
from django.conf import settings
from django.core.files import File
from django.db import models
from django.db.models.signals import post_delete
from django.urls import reverse
from django.utils.html import escape
from django.utils.translation import ugettext_lazy as _
from docutils.utils import SystemMessage
from model_utils import Choices
from model_utils.fields import AutoCreatedField, AutoLastModifiedField
from model_utils.models import TimeStampedModel
from natsort import natsorted

from localshop.apps.packages.utils import delete_files


logger = logging.getLogger(__name__)


class Repository(TimeStampedModel):

    name = models.CharField(max_length=250)
    slug = models.CharField(
        max_length=200,
        unique=True,
    )
    description = models.CharField(
        max_length=500,
        blank=True,
    )
    teams = models.ManyToManyField(
        'accounts.Team',
        related_name='repositories',
        blank=True,
    )
    enable_auto_mirroring = models.BooleanField(default=True)
    upstream_pypi_url = models.CharField(
        max_length=500,
        blank=True,
        default='https://pypi.python.org/simple',
        help_text=_("The upstream pypi URL (default: https://pypi.python.org/simple)"))

    def __str__(self):
        return self.name

    @property
    def simple_index_url(self):
        return reverse(
            'packages:simple_index',
            kwargs={
                'repo': self.slug,
            },
        )

    def user_has_access(self, user):
        if user.is_superuser:
            return True

        if not user.is_authenticated:
            return False
        return self.teams.filter(members__user=user).exists()

    def check_user_role(self, user, roles):
        if user.is_superuser:
            return True

        if not user.is_authenticated:
            return False

        return self.teams.filter(
            members__user=user,
            members__role__in=roles
        ).exists()

    @property
    def upstream_pypi_url_api(self):
        if self.upstream_pypi_url == 'https://pypi.python.org/simple':
            return 'https://pypi.python.org/pypi'
        return self.upstream_pypi_url


class Classifier(models.Model):

    name = models.CharField(
        max_length=255,
        unique=True,
    )

    def __str__(self):
        return self.name


class Package(models.Model):

    created = AutoCreatedField(db_index=True)
    modified = AutoLastModifiedField()
    repository = models.ForeignKey(
        Repository,
        related_name='packages',
        on_delete=models.CASCADE,
    )
    name = models.SlugField(max_length=200)
    normalized_name = models.SlugField(max_length=200)
    #: Indicate if this package is local (a private package)
    is_local = models.BooleanField(default=False)
    is_trusted = models.BooleanField(default=False)
    #: Timestamp when we last retrieved the metadata
    update_timestamp = models.DateTimeField(null=True)
    owners = models.ManyToManyField(settings.AUTH_USER_MODEL)

    class Meta:
        ordering = ['name']
        unique_together = [
            ('repository', 'name')
        ]
        permissions = (
            ("view_package", "Can view package"),
        )

    def __str__(self):
        return self.name

    def get_absolute_url(self):
        return reverse('dashboard:package_detail', kwargs={
            'repo': self.repository.slug, 'name': self.name
        })

    def get_all_releases(self):
        result = {}
        for release in self.releases.all():
            files = dict((r.filename, r) for r in release.files.all())
            result[release.version] = (release, files)
        return result

    def get_ordered_releases(self):
        releases = list(self.releases.all())
        releases = natsorted(releases, key=lambda rel: rel.version, reverse=True)
        return releases

    @property
    def last_release(self):
        return self.releases.order_by('-created')[0]


class Release(models.Model):

    created = AutoCreatedField()
    modified = AutoLastModifiedField()
    author = models.CharField(
        max_length=128,
        blank=True,
    )
    author_email = models.CharField(
        max_length=255,
        blank=True,
    )
    classifiers = models.ManyToManyField(Classifier)
    description = models.TextField(blank=True)
    download_url = models.CharField(
        max_length=200,
        blank=True,
        null=True,
    )
    home_page = models.CharField(
        max_length=200,
        blank=True,
        null=True,
    )
    license = models.TextField(blank=True)
    metadata_version = models.CharField(
        max_length=64,
        default=1.0,
    )
    package = models.ForeignKey(
        Package,
        related_name="releases",
        on_delete=models.CASCADE,
    )
    summary = models.TextField(blank=True)
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        on_delete=models.CASCADE,
    )
    version = models.CharField(max_length=512)

    class Meta:
        ordering = ['-version']

    def __str__(self):
        return self.version

    @property
    def description_html(self):
        try:
            parts = docutils.core.publish_parts(self.description, writer_name='html4css1')
            return parts['fragment']
        except SystemMessage:
            desc = escape(self.description)
            return '<pre>%s</pre>' % desc


def release_file_upload_to(instance, filename):
    package = instance.release.package
    assert package.name and instance.python_version
    return os.path.join(
        instance.release.package.repository.slug,
        instance.python_version,
        package.name[0],
        package.name,
        filename,
    )


class ReleaseFile(models.Model):

    TYPES = Choices(
        ('sdist', 'Source'),
        ('bdist_egg', 'Egg'),
        ('bdist_msi', 'MSI'),
        ('bdist_dmg', 'DMG'),
        ('bdist_rpm', 'RPM'),
        ('bdist_dumb', 'bdist_dumb'),
        ('bdist_wininst', 'bdist_wininst'),
        ('bdist_wheel', 'bdist_wheel'),
    )

    created = AutoCreatedField()
    modified = AutoLastModifiedField()
    upstream_pypi_upload_time = models.DateTimeField(null=True, blank=True)
    release = models.ForeignKey(
        Release,
        related_name="files",
        on_delete=models.CASCADE,
    )
    size = models.IntegerField(null=True)
    filetype = models.CharField(
        max_length=25,
        choices=TYPES,
    )
    distribution = models.FileField(
        upload_to=release_file_upload_to,
        max_length=512,
    )
    filename = models.CharField(
        max_length=255,
        blank=True,
        null=True,
    )
    md5_digest = models.CharField(max_length=512)
    python_version = models.CharField(max_length=255)
    requires_python = models.CharField(
        max_length=255,
        blank=True,
        null=True,
    )
    url = models.CharField(
        max_length=1024,
        blank=True,
    )
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        on_delete=models.CASCADE,
    )

    class Meta:
        unique_together = (
            'release',
            'filetype',
            'python_version',
            'filename',
        )

    def __str__(self):
        return self.filename

    def get_absolute_url(self):
        url = reverse(
            'packages:download',
            kwargs={
                'repo': self.release.package.repository.slug,
                'name': self.release.package.name,
                'pk': self.pk, 'filename': self.filename,
            },
        )
        return '%s#md5=%s' % (url, self.md5_digest)

    def save_filecontent(self, filename, fh):
        tmp_file = NamedTemporaryFile()
        copyfileobj(fh, tmp_file)
        self.distribution.save(filename, File(tmp_file))

    @property
    def file_is_available(self):
        return self.distribution and self.distribution.storage.exists(self.distribution.name)

    def download(self):
        """
        Start a celery task to download the release file from pypi.

        If `settings.LOCALSHOP_ISOLATED` is True then download the file
        in-process.

        """
        from .tasks import download_file
        if not settings.LOCALSHOP_ISOLATED:
            download_file.delay(pk=self.pk)
        else:
            download_file(pk=self.pk)


if settings.LOCALSHOP_DELETE_FILES:
    post_delete.connect(
        receiver=delete_files,
        sender=ReleaseFile,
        dispatch_uid="localshop.apps.packages.utils.delete_files",
    )
