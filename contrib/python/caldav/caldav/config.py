import json
import logging
import os

"""
This configuration parsing code was just copied from my plann library (and will be removed from there at some point in the future).  Test coverage is poor as for now.
"""

## This is being moved from my plann library.  The code itself will be introduced into caldav 2.0, but proper test code and documentation will come in a later release (2.1?)


## TODO TODO TODO - write test code for all the corner cases
## TODO TODO TODO - write documentation of config format
def expand_config_section(config, section="default", blacklist=None):
    """
    In the "normal" case, will return [ section ]

    We allow:

    * * includes all sections in config file
    * "Meta"-sections in the config file with the keyword "contains" followed by a list of section names
    * Recursive "meta"-sections
    * Glob patterns (work_* for all sections starting with work_)
    * Glob patterns in "meta"-sections
    """
    ## Optimizating for a special case.  The results should be the same without this optimization.
    if section == "*":
        return [x for x in config if not config[x].get("disable", False)]

    ## If it's not a glob-pattern ...
    if set(section).isdisjoint(set("[*?")):
        ## If it's referring to a "meta section" with the "contains" keyword
        if "contains" in config[section]:
            results = []
            if not blacklist:
                blacklist = set()
            blacklist.add(section)
            for subsection in config[section]["contains"]:
                if not subsection in results and not subsection in blacklist:
                    for recursivesubsection in expand_config_section(
                        config, subsection, blacklist
                    ):
                        if not recursivesubsection in results:
                            results.append(recursivesubsection)
            return results
        else:
            ## Disabled sections should be ignored
            if config.get("section", {}).get("disable", False):
                return []

            ## NORMAL CASE - return [ section ]
            return [section]
    ## section name is a glob pattern
    matching_sections = [x for x in config if fnmatch(x, section)]
    results = set()
    for s in matching_sections:
        if set(s).isdisjoint(set("[*?")):
            results.update(expand_config_section(config, s))
        else:
            ## Section names shouldn't contain []?* ... but in case they do ... don't recurse
            results.add(s)
    return results


def config_section(config, section="default"):
    if section in config and "inherits" in config[section]:
        ret = config_section(config, config[section]["inherits"])
    else:
        ret = {}
    if section in config:
        ret.update(config[section])
    return ret


def read_config(fn, interactive_error=False):
    if not fn:
        cfgdir = f"{os.environ.get('HOME', '/')}/.config/"
        for config_file in (
            f"{cfgdir}/caldav/calendar.conf",
            f"{cfgdir}/caldav/calendar.yaml",
            f"{cfgdir}/caldav/calendar.json",
            f"{cfgdir}/calendar.conf",
            "/etc/calendar.conf",
            "/etc/caldav/calendar.conf",
        ):
            cfg = read_config(config_file)
            if cfg:
                return cfg
        return None

    ## This can probably be refactored into fewer lines ...
    try:
        try:
            with open(fn, "rb") as config_file:
                return json.load(config_file)
        except json.decoder.JSONDecodeError:
            ## Late import, wrapped in try/except.  yaml is external module,
            ## and not included in the requirements as for now.
            try:
                import yaml

                try:
                    with open(fn, "rb") as config_file:
                        return yaml.load(config_file, yaml.Loader)
                except yaml.scanner.ScannerError:
                    logging.error(
                        f"config file {fn} exists but is neither valid json nor yaml.  Check the syntax."
                    )
            except ImportError:
                logging.error(
                    f"config file {fn} exists but is not valid json, and pyyaml is not installed."
                )

    except FileNotFoundError:
        ## File not found
        logging.info("no config file found")
    except ValueError:
        if interactive_error:
            logging.error(
                "error in config file.  Be aware that the interactive configuration will ignore and overwrite the current broken config file",
                exc_info=True,
            )
        else:
            logging.error("error in config file.  It will be ignored", exc_info=True)
    return {}
