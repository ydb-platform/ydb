import dataclasses
import logging
from typing import List

import requests


@dataclasses.dataclass
class Runner:
    id: int
    name: str
    online: bool
    busy: bool
    tags: List[str]

    @property
    def instance_id(self):
        for t in self.tags:
            if t.startswith("instance:"):
                return t.replace("instance:", "")
        raise ValueError("Unable to find instance_id")

    @property
    def full_name(self):
        return f"#{self.id} {self.name}"

    @property
    def offline(self):
        return not self.online

    def has_tag(self, tag):
        return tag in self.tags


class RunnerList:
    def __init__(self):
        self.runners = []

    def add(self, runner: Runner):
        self.runners.append(runner)

    def __len__(self):
        return len(self.runners)

    @property
    def online(self):
        c = 0
        for r in self.runners:
            if r.online:
                c += 1
        return c

    @property
    def busy(self):
        c = 0
        for r in self.runners:
            if r.busy:
                c += 1
        return c

    @property
    def available(self):
        c = 0
        for r in self.runners:
            if r.online and not r.busy:
                c += 1
        return c


class Github:
    def __init__(self, repo, access_token):
        self.repo = repo
        self.api_url = f"https://api.github.com/repos/{repo}"
        self.html_url = f"https://github.com/{repo}"
        self.access_token = access_token
        self.session = requests.session()
        self.logger = logging.getLogger(__name__)

    def _request(self, method, url, params=None, data=None):
        headers = {
            "Authorization": f"token {self.access_token}",
            "Accept": "application/vnd.github.v3+json",
        }
        response = getattr(self.session, method)(url, params=params, json=data, headers=headers)
        response.raise_for_status()
        result = None
        if response.status_code != 204:
            result = response.json()
        return result

    def _get(self, method, params=None):
        return self._request("get", f"{self.api_url}/{method}", params=params)

    def _post(self, method, data=None, params=None):
        return self._request("post", f"{self.api_url}/{method}", data=data, params=params)

    def _delete(self, method, params=None):
        return self._request("delete", f"{self.api_url}/{method}", params=params)

    def get_runners(self):
        result = []
        page = 1

        online_count = busy_count = 0
        while 1:
            params = {"page": str(page), "per_page": "100"}
            data = self._get(f"actions/runners", params)

            for runner in data["runners"]:
                tags = [tag["name"] for tag in runner["labels"]]
                is_online = runner["status"] == "online"
                is_busy = runner["busy"]
                if is_online:
                    online_count += 1
                if is_busy:
                    busy_count += 1

                result.append(
                    Runner(
                        id=runner["id"],
                        name=runner["name"],
                        tags=tags,
                        online=is_online,
                        busy=is_busy,
                    )
                )

            if data["total_count"] <= len(result) or not data["runners"]:
                break
            else:
                page += 1
        self.logger.info("got %s runners, online=%s, busy=%s", len(result), online_count, busy_count)

        return result

    def get_new_runner_token(self):
        data = self._post(f"actions/runners/registration-token")
        return data["token"]

    def delete_runner(self, runner_id):
        data = self._delete(f"actions/runners/{runner_id}")
        return data
