"""Nomad region: https://developer.hashicorp.com/nomad/api-docs/regions"""

import nomad.api.exceptions

from nomad.api.base import Requester


class Regions(Requester):
    """
    https://www.nomadproject.io/docs/http/regions.html
    """

    ENDPOINT = "regions"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __str__(self):
        return f"{self.__dict__}"

    def __repr__(self):
        return f"{self.__dict__}"

    def __getattr__(self, item):
        msg = f"{item} does not exist"
        raise AttributeError(msg)

    def __contains__(self, item):
        try:
            regions = self.get_regions()

            for region in regions:
                if region == item:
                    return True
            return False
        except nomad.api.exceptions.URLNotFoundNomadException:
            return False

    def __len__(self):
        regions = self.get_regions()
        return len(regions)

    def __getitem__(self, item):
        try:
            regions = self.get_regions()

            for region in regions:
                if region == item:
                    return region
            raise KeyError
        except nomad.api.exceptions.URLNotFoundNomadException as exc:
            raise KeyError from exc

    def __iter__(self):
        regions = self.get_regions()
        return iter(regions)

    def get_regions(self):
        """Returns the known region names.

        https://www.nomadproject.io/docs/http/regions.html

        returns: list
        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """
        return self.request(method="get").json()
