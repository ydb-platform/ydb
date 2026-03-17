class AzureEnvironment(object):

    Global = "Global"
    """Referred to as Azure or Azure Global."""

    USGovernment = "GCC"
    """Government Community Cloud"""

    USGovernmentHigh = "GCC High"
    """Government Community Cloud High."""

    China = "CN"
    """Operated by 21Vianet under Chinese regulations."""

    Germany = "Azure DE"
    """Legacy; for data sovereignty in Germany."""

    USGovernmentDoD = "DoD"
    """Azure for U.S. Department of Defense (DoD)"""

    _authority_endpoints = {
        Global: {
            "graph": "https://graph.microsoft.com",
            "login": "https://login.microsoftonline.com",
        },
        USGovernment: {
            "graph": "https://graph.microsoft.com",
            "login": "https://login.microsoftonline.com",
        },
        USGovernmentHigh: {
            "graph": "https://graph.microsoft.us",
            "login": "https://login.microsoftonline.us",
        },
        USGovernmentDoD: {
            "graph": "https://dod-graph.microsoft.us",
            "login": "https://login.microsoftonline.us",
        },
        Germany: {
            "graph": "https://graph.microsoft.de",
            "login": "https://login.microsoftonline.de",
        },
        China: {
            "graph": "https://microsoftgraph.chinacloudapi.cn",
            "login": "https://login.chinacloudapi.cn",
        },
    }

    @classmethod
    def get_graph_authority(cls, env):
        # type: (str) -> str
        return cls._authority_endpoints.get(env, cls._authority_endpoints["Global"])[
            "graph"
        ]

    @classmethod
    def get_login_authority(cls, env):
        # type: (str) -> str
        return cls._authority_endpoints.get(env, cls._authority_endpoints["Global"])[
            "login"
        ]
