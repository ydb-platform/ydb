from typing import Any
from typing import Dict
from typing import Tuple

from django.db import models
from django.utils.translation import gettext_lazy as _

from . import ShortUUID


class ShortUUIDField(models.CharField):
    description = _("A short UUID field.")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.length: int = kwargs.pop("length", 22)  # type: ignore
        self.prefix: str = kwargs.pop("prefix", "")  # type: ignore

        if "max_length" not in kwargs:
            # If `max_length` was not specified, set it here.
            kwargs["max_length"] = self.length + len(self.prefix)  # type: ignore

        self.alphabet: str = kwargs.pop("alphabet", None)  # type: ignore
        kwargs["default"] = self._generate_uuid  # type: ignore

        super().__init__(*args, **kwargs)

    def _generate_uuid(self) -> str:
        """Generate a short random string."""
        return self.prefix + ShortUUID(alphabet=self.alphabet).random(
            length=self.length
        )

    def deconstruct(self) -> Tuple[str, str, Tuple, Dict[str, Any]]:
        name, path, args, kwargs = super().deconstruct()
        kwargs["alphabet"] = self.alphabet
        kwargs["length"] = self.length
        kwargs["prefix"] = self.prefix
        kwargs.pop("default", None)
        return name, path, args, kwargs
