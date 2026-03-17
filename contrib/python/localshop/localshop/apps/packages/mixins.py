from django.shortcuts import get_object_or_404

from localshop.apps.packages.models import Repository


class RepositoryMixin(object):
    def dispatch(self, request, *args, **kwargs):
        self.repository = get_object_or_404(
            Repository.objects, slug=kwargs['repo'])
        return super().dispatch(request, *args, **kwargs)
