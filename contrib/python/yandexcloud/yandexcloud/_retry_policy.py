import json
from enum import Enum
from typing import Any, Dict, Tuple

import grpc


class ThrottlingMode(str, Enum):
    PERSISTENT = "persistent"
    TEMPORARY = "temporary"


def get_throttling_policy(throttling_mode: ThrottlingMode) -> Dict[str, Any]:
    if throttling_mode == ThrottlingMode.PERSISTENT:
        return {"maxTokens": 100, "tokenRatio": 0.1}

    return {"maxTokens": 6, "tokenRatio": 0.1}


class RetryPolicy:
    def __init__(
        self,
        max_attempts: int = 4,
        status_codes: Tuple["grpc.StatusCode"] = (grpc.StatusCode.UNAVAILABLE,),
        throttling_mode: ThrottlingMode = ThrottlingMode.TEMPORARY,
    ):
        self.__policy = {
            "methodConfig": [
                {
                    "name": [{}],
                    "retryPolicy": {
                        "maxAttempts": max_attempts,
                        "initialBackoff": "0.1s",
                        "maxBackoff": "20s",
                        "backoffMultiplier": 2,
                        "retryableStatusCodes": [status.name for status in status_codes],
                    },
                    "waitForReady": True,
                }
            ],
            "retryThrottling": get_throttling_policy(throttling_mode),
        }

    def to_json(self) -> str:
        return json.dumps(self.__policy)
