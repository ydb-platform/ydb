from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

try:
    import httpx
except ImportError as exc:  # pragma: no cover
    msg = "Please run `$pip install 'datamodel-code-generator[http]`' to resolve URL Reference"
    raise Exception(msg) from exc  # noqa: TRY002


def get_body(
    url: str,
    headers: Sequence[tuple[str, str]] | None = None,
    ignore_tls: bool = False,  # noqa: FBT001, FBT002
    query_parameters: Sequence[tuple[str, str]] | None = None,
) -> str:
    return httpx.get(
        url,
        headers=headers,
        verify=not ignore_tls,
        follow_redirects=True,
        params=query_parameters,  # pyright: ignore[reportArgumentType]
        # TODO: Improve params type
    ).text


def join_url(url: str, ref: str = ".") -> str:
    return str(httpx.URL(url).join(ref))
