# don't need anymore defer to mf2util instead (mf2util does not have this functionality)


def get_url(mf):
    """Given a property value that may be a list of simple URLs or complex
    h-* dicts (with a url property), extract a list of URLs. This is useful
    when parsing e.g., in-reply-to.

    Args:
      mf (string or dict): URL or h-cite-style dict

    Returns:
      list: a list of URLs
    """

    urls = []
    for item in mf:
        if isinstance(item, str):
            urls.append(item)
        elif isinstance(item, dict) and any(
            x.startswith("h-") for x in item.get("type", [])
        ):
            urls.extend(item.get("properties", {}).get("url", []))

    return urls


def unordered_list(l):
    """given a list, returns another list with unique and alphabetically sorted elements.
    use for HTML attributes that have no semantics to their order e.g. class, rel.
    """
    return sorted(set(l))
