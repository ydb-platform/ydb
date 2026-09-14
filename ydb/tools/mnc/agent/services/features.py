from enum import Enum
from typing import Dict, List, Optional


class FeatureStatus(Enum):
    ENABLED = "enabled"
    DISABLED = "disabled"
    EXPERIMENTAL = "experimental"


class FeatureService:
    def __init__(self):
        self._features: Dict[str, FeatureStatus] = {}

    def get_feature_status(self, feature_name: str) -> Optional[FeatureStatus]:
        return self._features.get(feature_name)

    def set_feature_status(self, feature_name: str, status: FeatureStatus) -> None:
        self._features[feature_name] = status

    def get_all_features(self) -> Dict[str, str]:
        return {name: status.value for name, status in self._features.items()}

    def get_enabled_features(self) -> List[str]:
        return [name for name, status in self._features.items() if status == FeatureStatus.ENABLED]

    def is_feature_enabled(self, feature_name: str) -> bool:
        return self.get_feature_status(feature_name) == FeatureStatus.ENABLED


features_service = FeatureService()
