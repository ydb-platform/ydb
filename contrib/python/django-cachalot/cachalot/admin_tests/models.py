from django.conf import settings
from django.db.models import Q, UniqueConstraint, Model, CharField, ForeignKey, SET_NULL


class TestModel(Model):
    name = CharField(max_length=20)
    owner = ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True,
                       on_delete=SET_NULL)

    class Meta:
        ordering = ('name',)
        constraints = [
            UniqueConstraint(
                fields=["name"],
                condition=Q(owner=None),
                name="unique_name",
            )
        ]
