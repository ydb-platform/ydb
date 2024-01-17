import json
import os
from typing import Set

MDS_PREFIX = "https://storage.yandex-team.ru/get-devtools/"
UNIVERSAL_PREFIX = "https://{canondata_backend}/"


def get_object_name(url: str):
    return url.split("#", maxsplit=1)[0].replace(MDS_PREFIX, "").replace(UNIVERSAL_PREFIX, "")


def is_canondata_url(url: str):
    return url.startswith(MDS_PREFIX) or url.startswith(UNIVERSAL_PREFIX)


def get_urls(root_dir: str) -> Set[str]:
    urls = set()
    for root, subFolders, files in os.walk(root_dir):
        for f in files:
            if f != "result.json":
                continue

            fn = os.path.join(root, f)

            with open(fn, "r") as fp:
                queue = list(json.load(fp).values())

                while queue:
                    item = queue.pop()

                    if isinstance(item, dict):
                        if "uri" in item:
                            if is_canondata_url(item["uri"]):
                                urls.add(get_object_name(item["uri"]))
                        else:
                            queue.extend(item.values())
                    elif isinstance(item, list):
                        queue.extend(item[:])
    return urls
