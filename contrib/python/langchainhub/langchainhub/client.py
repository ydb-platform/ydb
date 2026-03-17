import json
import os
import requests
import socket
from typing import Optional, Dict, Union, Tuple, List
from urllib.parse import urlsplit
from langchainhub import _types
from packaging import version
from warnings import warn, simplefilter

simplefilter('always', DeprecationWarning)

LS_VERSION_WITH_OPTMIZATION = "0.5.23"

def deprecation_warning(suggestion: str):
    message = (
        "The `langchainhub sdk` is deprecated.\n"
        "Please use the `langsmith sdk` instead:\n"
        "  pip install langsmith\n"
        f"{suggestion}"
    )
    warn(message, DeprecationWarning, stacklevel=3)

def _is_localhost(url: str) -> bool:
    """Check if the URL is localhost.

    Parameters
    ----------
    url : str
        The URL to check.

    Returns
    -------
    bool
        True if the URL is localhost, False otherwise.
    """
    try:
        netloc = urlsplit(url).netloc.split(":")[0]
        ip = socket.gethostbyname(netloc)
        return ip == "127.0.0.1" or ip.startswith("0.0.0.0") or ip.startswith("::")
    except socket.gaierror:
        return False


def _get_api_key(api_key: Optional[str]) -> Optional[str]:
    api_key = (
        api_key
        or os.getenv("LANGCHAIN_HUB_API_KEY")
        or os.getenv("LANGCHAIN_API_KEY")
        or os.getenv("LANGSMITH_API_KEY")
    )
    if api_key is None or not api_key.strip():
        return None
    return api_key.strip().strip('"').strip("'") or None


def _get_api_url(api_url: Optional[str]) -> str:
    default_api_url = "https://api.hub.langchain.com"
    _api_url = (
        api_url
        if api_url is not None
        else os.getenv(
            "LANGCHAIN_HUB_API_URL",
            default_api_url,
        )
    )
    if not _api_url.strip():
        raise ValueError("LangChain Hub API URL cannot be empty")
    return _api_url.strip().strip('"').strip("'").rstrip("/")


def is_version_greater_or_equal(current_version, target_version):
    current = version.parse(current_version)
    target = version.parse(target_version)
    return current >= target


def parse_owner_repo_commit(identifier: str) -> Tuple[str, str, str]:
    """
    Parses a string in the format of `owner/repo:commit` and returns a tuple of
    (owner, repo, commit).
    """
    owner_repo = identifier
    commit = "latest"
    if ":" in identifier:
        owner_repo, commit = identifier.split(":", 1)

    if "/" not in owner_repo:
        return "-", owner_repo, commit

    owner, repo = owner_repo.split("/", 1)
    return owner, repo, commit


class Client:
    """
    An API Client for LangChainHub
    """

    def __init__(self, api_url: Optional[str] = None, *, api_key: Optional[str] = None):
        self.api_key = _get_api_key(api_key)
        self.api_url = _get_api_url(api_url)

    def _get_headers(self):
        headers = {}
        if self.api_key is not None:
            headers["x-api-key"] = self.api_key
        return headers

    @property
    def _get_ls_info(self):
        res = requests.get(f"{self.api_url}/info").json()
        return res

    @property
    def _host_url(self) -> str:
        """The web host url."""
        if _is_localhost(self.api_url):
            link = "http://localhost"
        elif "beta" in self.api_url.split(".", maxsplit=1)[0]:
            link = "https://beta.smith.langchain.com"
        elif "dev" in self.api_url.split(".", maxsplit=1)[0]:
            link = "https://dev.smith.langchain.com"
        else:
            link = "https://smith.langchain.com"
        return link

    def get_settings(self):
        res = requests.get(
            f"{self.api_url}/settings",
            headers=self._get_headers(),
        )
        res.raise_for_status()
        return res.json()

    def set_tenant_handle(self, tenant_handle: str):
        res = requests.post(
            f"{self.api_url}/settings/handle",
            headers=self._get_headers(),
            json={"tenant_handle": tenant_handle},
        )
        res.raise_for_status()
        return res.json()

    def list_repos(self, limit: int = 100, offset: int = 0):
        deprecation_warning("Use the `list_prompts` method.")
        res = requests.get(
            f"{self.api_url}/repos?limit={limit}&offset={offset}",
            headers=self._get_headers(),
        )
        res.raise_for_status()
        return res.json()

    def get_repo(self, repo_full_name: str):
        res = requests.get(
            f"{self.api_url}/repos/{repo_full_name}",
            headers=self._get_headers(),
        )
        res.raise_for_status()
        return res.json()

    def create_repo(
        self, repo_handle: str, *, description: str = "", is_public: bool = True
    ):
        json = {
            "repo_handle": repo_handle,
            "is_public": is_public,
            "description": description,
        }
        res = requests.post(
            f"{self.api_url}/repos/",
            headers=self._get_headers(),
            json=json,
        )
        res.raise_for_status()
        return res.json()

    def list_commits(self, repo_full_name: str, limit: int = 100, offset: int = 0):
        res = requests.get(
            f"{self.api_url}/commits/{repo_full_name}/?limit={limit}&offset={offset}",
            headers=self._get_headers(),
        )
        res.raise_for_status()
        return res.json()

    def like_repo(self, repo_full_name: str):
        res = requests.post(
            f"{self.api_url}/likes/{repo_full_name}",
            headers=self._get_headers(),
            json={"like": True},
        )
        res.raise_for_status()
        return res.json()

    def unlike_repo(self, repo_full_name: str):
        res = requests.post(
            f"{self.api_url}/likes/{repo_full_name}",
            headers=self._get_headers(),
            json={"like": False},
        )
        res.raise_for_status()
        return res.json()

    def _get_latest_commit_hash(self, repo_full_name: str) -> Optional[str]:
        commits_resp = self.list_commits(repo_full_name)
        commits = commits_resp["commits"]
        if len(commits) == 0:
            return None
        return commits[0]["commit_hash"]

    def update_repo(
        self,
        repo_full_name: str,
        *,
        description: Optional[str] = None,
        is_public: Optional[bool] = None,
        tags: Optional[List[str]] = None,
    ):
        json: Dict[str, Union[str, bool, List[str]]] = {}
        if description is not None:
            json["description"] = description
        if is_public is not None:
            json["is_public"] = is_public
        if tags is not None:
            json["tags"] = tags
        res = requests.patch(
            f"{self.api_url}/repos/{repo_full_name}",
            headers=self._get_headers(),
            json=json,
        )
        res.raise_for_status()
        return res.json()

    def push(
        self,
        repo_full_name: str,
        manifest_json: str,
        *,
        parent_commit_hash: Optional[str] = "latest",
        new_repo_is_public: bool = False,
        new_repo_description: str = "",
    ):
        deprecation_warning("Use the `push_prompt` method.")
        settings = self.get_settings()
        if new_repo_is_public:
            if not settings["tenant_handle"]:
                raise ValueError(
                    """
                  Cannot create public prompt without first creating a LangChain Hub handle.
                  
                  You can add a handle by creating a public prompt at:
                      https://smith.langchain.com/prompts
                  
                  This is a workspace-level handle and will be associated with all of your workspace's public prompts in the LangChain Hub.
                  """
                )
        owner, repo_handle, _ = parse_owner_repo_commit(repo_full_name)
        full_repo = f"{owner}/{repo_handle}"
        try:
            # check if the repo exists
            _ = self.get_repo(full_repo)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code != 404:
                raise e
            # create repo if it doesn't exist
            # make sure I am owner if owner is specified
            if (
                settings["tenant_handle"]
                and owner != "-"
                and settings["tenant_handle"] != owner
            ):
                raise ValueError(
                    f"Tenant {settings['tenant_handle']} is not the owner of repo {repo_full_name}"
                )
            self.create_repo(
                repo_handle,
                is_public=new_repo_is_public,
                description=new_repo_description,
            )

        manifest_dict = json.loads(manifest_json)
        if parent_commit_hash == "latest":
            parent_commit_hash = self._get_latest_commit_hash(full_repo)
        request_dict = {"parent_commit": parent_commit_hash, "manifest": manifest_dict}
        res = requests.post(
            f"{self.api_url}/commits/{full_repo}",
            headers=self._get_headers(),
            json=request_dict,
        )
        res.raise_for_status()
        res = res.json()
        commit_hash = res["commit"]["commit_hash"]
        short_hash = commit_hash[:8]
        url = (
            self._host_url
            + f"/prompts/{repo_handle}/{short_hash}?organizationId={settings['id']}"
        )
        return url

    def pull_repo(self, owner_repo_commit: str) -> _types.Repo:
        deprecation_warning("Use the `pull_prompt` method.")
        info = self._get_ls_info
        ls_version = info["version"]
        use_optimization = is_version_greater_or_equal(
            current_version=ls_version, target_version=LS_VERSION_WITH_OPTMIZATION
        )

        owner, repo, commit_hash = parse_owner_repo_commit(owner_repo_commit)

        if not use_optimization:
            if commit_hash is None or commit_hash == "latest":
                commit_hash = self._get_latest_commit_hash(f"{owner}/{repo}")
                if commit_hash is None:
                    raise ValueError("No commits found")

        res = requests.get(
            f"{self.api_url}/commits/{owner}/{repo}/{commit_hash}",
            headers=self._get_headers(),
        )
        res.raise_for_status()
        result = res.json()
        return {"owner": owner, "repo": repo, **result}

    def pull(self, owner_repo_commit: str):
        deprecation_warning("Use the `pull_prompt` method.")
        res_dict = self.pull_repo(owner_repo_commit)
        return json.dumps(res_dict["manifest"])
