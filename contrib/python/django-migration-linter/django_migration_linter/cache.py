from __future__ import annotations

import os
import pickle


class Cache(dict):
    def __init__(self, django_folder: str | None, database: str, cache_path: str):
        self.filename = os.path.join(
            cache_path,
            "{}_{}.pickle".format(str(django_folder).replace(os.sep, "_"), database),
        )

        if not os.path.exists(os.path.dirname(self.filename)):
            os.makedirs(os.path.dirname(self.filename))

        super().__init__()

    def load(self) -> None:
        try:
            with open(self.filename, "rb") as f:
                tmp_dict = pickle.load(f)
                self.update(tmp_dict)
        except OSError:
            pass

    def save(self) -> None:
        with open(self.filename, "wb") as f:
            pickle.dump(self, f, protocol=2)
