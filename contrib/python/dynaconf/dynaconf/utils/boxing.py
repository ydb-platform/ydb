from __future__ import annotations

import inspect
from typing import Any

from dynaconf.utils import find_the_correct_casing
from dynaconf.utils import recursively_evaluate_lazy_format
from dynaconf.utils.functional import empty
from dynaconf.vendor.box import Box


class DynaBox(Box):
    """Specialized Box for dynaconf
    it allows items/attrs to be found both in upper or lower case"""

    def __getattr__(self, item, *args, **kwargs):
        try:
            result = super().__getattr__(item, *args, **kwargs)
        except (AttributeError, KeyError):
            all_keys = tuple(self.keys())
            n_item = find_the_correct_casing(item, all_keys) or item
            result = super().__getattr__(n_item, *args, **kwargs)
        return self.__evaluate_lazy__(result)

    def __getitem__(self, item, *args, **kwargs):
        try:
            result = super().__getitem__(item, *args, **kwargs)
        except (AttributeError, KeyError):
            all_keys = tuple(self.keys())
            n_item = find_the_correct_casing(item, all_keys) or item
            result = super().__getitem__(n_item, *args, **kwargs)
        return self.__evaluate_lazy__(result)

    def get(
        self, item, default=None, bypass_eval=False, *args, **kwargs
    ) -> Any:
        # _TODO(pbrochad): refactor all these getter methods to make consistency easier
        if not bypass_eval:
            all_keys = tuple(self.keys())
            n_item = find_the_correct_casing(item, all_keys) or item
            result = super().get(n_item, empty, *args, **kwargs)
            result = result if result is not empty else default
            return self.__evaluate_lazy__(result)
        try:
            return super().__getitem__(item, *args, **kwargs)
        except (AttributeError, KeyError):
            all_keys = tuple(self.keys())
            n_item = find_the_correct_casing(item, all_keys) or item
            return super().__getitem__(n_item, *args, **kwargs)

    def __evaluate_lazy__(self, result):
        settings = self._box_config["box_settings"]
        return recursively_evaluate_lazy_format(result, settings)

    def __copy__(self):
        return self.__class__(
            super(Box, self).copy(),
            box_settings=self._box_config.get("box_settings"),
        )

    def copy(self, bypass_eval=False):
        if not bypass_eval:
            return self.__class__(
                super(Box, self).copy(),
                box_settings=self._box_config.get("box_settings"),
            )
        return self.__class__(
            {k: self.get(k, bypass_eval=True) for k in self.keys()},
            box_settings=self._box_config.get("box_settings"),
        )

    def __dir__(self):
        keys = list(self.keys())
        reserved = [
            item[0]
            for item in inspect.getmembers(DynaBox)
            if not item[0].startswith("__")
        ]
        return (
            keys
            + [k.lower() for k in keys]
            + [k.upper() for k in keys]
            + reserved
        )
