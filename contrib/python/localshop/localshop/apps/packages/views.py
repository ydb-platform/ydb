import logging
from urllib.request import urlopen
from wsgiref.util import FileWrapper

from braces.views import CsrfExemptMixin
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist
from django.http import (
    Http404,
    HttpResponse,
    HttpResponseBadRequest,
    HttpResponseForbidden,
    HttpResponseNotFound,
    StreamingHttpResponse,
)
from django.shortcuts import redirect
from django.urls import reverse
from django.utils import six
from django.views import generic
from packaging.version import InvalidVersion, Version

from localshop.apps.packages import forms, models
from localshop.apps.packages.mixins import RepositoryMixin
from localshop.apps.packages.pypi import normalize_name
from localshop.apps.packages.tasks import fetch_package
from localshop.apps.packages.utils import alter_old_distutils_request
from localshop.apps.permissions.mixins import RepositoryAccessMixin
from localshop.utils import enqueue


logger = logging.getLogger(__name__)


class SimpleIndex(CsrfExemptMixin, RepositoryMixin, RepositoryAccessMixin,
                  generic.ListView):
    """
    Index view with all available packages used by /simple url

    This page is used by pip/easy_install to find packages.
    """
    context_object_name = 'packages'
    http_method_names = ['get', 'post']
    template_name = 'packages/simple_package_list.html'

    def post(self, request, repo):
        alter_old_distutils_request(request)

        actions = {
            'submit': handle_register_or_upload,
            'file_upload': handle_register_or_upload,
        }
        action = request.POST.get(':action')

        handler = actions.get(action)
        if not handler:
            logger.info(f"Unknown action: {action}.")
            return HttpResponseNotFound('Unknown action: %s' % action)

        if not request.user.is_authenticated and not request.credentials:
            logger.info(f"User '{request.user}' is not authenticated.")
            return HttpResponseForbidden(
                "You need to be authenticated to upload packages")

        # Both actions currently are upload actions, so check is simple
        if request.credentials and not request.credentials.allow_upload:
            logger.info(
                f"Provided access_key does not allow uploading packages {request.credentials.access_key} "
                f"for user '{request.user}'."
            )
            return HttpResponseForbidden(
                "Upload is not allowed with the provided credentials")

        return handler(
            request.POST, request.FILES, request.user, self.repository)

    def get_queryset(self):
        return self.repository.packages.all()


class SimpleDetail(RepositoryMixin, RepositoryAccessMixin, generic.DetailView):
    """
    List all available files for a specific package.

    This page is used by pip/easy_install to find the files.
    """
    model = models.Package
    context_object_name = 'package'
    template_name = 'packages/simple_package_detail.html'

    def get(self, request, repo, slug):
        try:
            package = self.repository.packages.get(
                normalized_name=normalize_name(slug)
            )
        except ObjectDoesNotExist:
            if not self.repository.enable_auto_mirroring:
                raise Http404("Auto mirroring is not enabled")

            enqueue(fetch_package, self.repository.pk, slug)
            return redirect(self.repository.upstream_pypi_url + '/' + slug)

        # Redirect if slug is not an exact match
        if slug != package.normalized_name:
            url = reverse('packages:simple_detail', kwargs={
                'repo': self.repository.slug,
                'slug': package.normalized_name
            })
            return redirect(url)

        self.object = package
        context = self.get_context_data(
            object=self.object,
            releases=list(package.releases.prefetch_related('files')))
        return self.render_to_response(context)

    def get_context_data(self, *args, **kwargs):
        ctx = super().get_context_data(*args, **kwargs)
        ctx['base_url'] = self.request.build_absolute_uri('/')[:-1]
        return ctx


class DownloadReleaseFile(RepositoryMixin, RepositoryAccessMixin,
                          generic.View):
    """
    If the requested file is not already cached locally from a previous
    download it will be fetched from PyPi for local storage and the client will
    be redirected to PyPi, unless the LOCALSHOP_ISOLATED variable is set to
    True, in which case the file will be served to the client after it is
    downloaded.
    """
    def get(self, request, repo, name, pk, filename):
        release_file = models.ReleaseFile.objects.get(pk=pk)
        if not release_file.file_is_available:
            if not self.repository.enable_auto_mirroring:
                raise Http404("Auto mirroring is not enabled")

            logger.info("Queueing %s for mirroring", release_file.url)
            release_file.download()
            if not settings.LOCALSHOP_ISOLATED:
                logger.debug("Redirecting user to pypi")
                return redirect(release_file.url)
            else:
                release_file = models.ReleaseFile.objects.get(pk=pk)

        if settings.MEDIA_URL:
            return redirect(release_file.distribution.url)

        content_type = 'application/force-download'

        if hasattr(settings, 'USE_ACCEL_REDIRECT'):
            use_accel_redirect = settings.USE_ACCEL_REDIRECT
        else:
            use_accel_redirect = False

        if use_accel_redirect:
            # Nginx-config must contain something like that:
            # location /.storage/ {
            #     internal;
            #     proxy_pass $arg_fileurl;
            #     proxy_hide_header Content-Type;
            # }
            response = HttpResponse(content='', content_type=content_type)
            url = release_file.distribution.url
            try:
                response['X-Accel-Redirect'] = settings.ACCEL_REDIRECT_SLUG + url
            except AttributeError as exc:
                logger.error('ACCEL_REDIRECT_SLUG should be defined')
                raise
            response['X-Accel-Buffering'] = 'yes'
        else:
            response = StreamingHttpResponse(
                urlopen(release_file.distribution.url),
                content_type=content_type,
            )

        # TODO: Use sendfile if enabled
        response['Content-Disposition'] = 'attachment; filename=%s' % (
            release_file.filename)

        if release_file.size:
            response["Content-Length"] = release_file.size

        return response


def handle_register_or_upload(post_data, files, user, repository):
    """
    Process a `register` or `upload` comment issued via distutils.

    This method is called with the authenticated user.
    """
    name = post_data.get('name')
    version = post_data.get('version')

    if settings.LOCALSHOP_VERSION_VALIDATION:
        try:
            Version(version)
        except InvalidVersion:
            logger.info(
                f"Invalid package version supplied '{version}' for '{name}' package."
            )
            response = HttpResponseBadRequest(
                reason="Invalid version supplied '{!s}' for '{!s}' package.".format(version, name))
            return response

    if not name or not version:
        logger.info(f"No name ({name}) or version ({version}) given for uploaded package.")
        return HttpResponseBadRequest('No name or version given')

    try:
        package = repository.packages.get(normalized_name=normalize_name(name))

        # Error out when we try to override a mirror'ed package for now
        # not sure what the best thing is
        if not package.is_local:
            logger.info(f"Trying to override a mirrored package '{package.name}'.")
            return HttpResponseBadRequest(
                '%s is a pypi package!' % package.name)

        try:
            release = package.releases.get(version=version)
        except ObjectDoesNotExist:
            release = None
    except ObjectDoesNotExist:
        package = None
        release = None

    # Validate the data
    form = forms.ReleaseForm(post_data, instance=release)
    if not form.is_valid():
        logger.info(f"Release form is invalid: {form.errors.values()}.")
        return HttpResponseBadRequest(reason=form.errors.values()[0][0])

    if not package:
        pkg_form = forms.PackageForm(post_data, repository=repository)
        if not pkg_form.is_valid():
            logger.info(f"Package form is invalid: {pkg_form.errors.values()}.")
            return HttpResponseBadRequest(
                reason=six.next(six.itervalues(pkg_form.errors))[0])
        package = pkg_form.save()

    release = form.save(commit=False)
    release.package = package
    release.save()

    # If this is an upload action then process the uploaded file
    if files:
        files = {
            'distribution': files['content']
        }
        filename = files['distribution']._name
        try:
            release_file = release.files.get(filename=filename)
            if settings.LOCALSHOP_RELEASE_OVERWRITE is False:
                logger.info(f"Version '{release.version}' of package '{package.name}' already exists.")
                message = 'That it already released, please bump version.'
                return HttpResponseBadRequest(message)
        except ObjectDoesNotExist:
            release_file = models.ReleaseFile(
                release=release, filename=filename)

        form_file = forms.ReleaseFileForm(
            post_data, files, instance=release_file)
        if not form_file.is_valid():
            logger.info(f"ReleaseFile form is invalid: {form_file.errors.values()}")
            return HttpResponseBadRequest('ERRORS %s' % form_file.errors)
        release_file = form_file.save(commit=False)
        release_file.save()

    return HttpResponse()
