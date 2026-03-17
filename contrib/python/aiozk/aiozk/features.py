ALL_FEATURES = {
    'create_with_stat': (3, 5, 0),
    'containers': (3, 5, 1),
    'reconfigure': (3, 5, 0),
}


class Features:
    def __init__(self, version_info):
        for feature_name, version_introduced in ALL_FEATURES.items():
            setattr(self, feature_name, version_info >= version_introduced)
