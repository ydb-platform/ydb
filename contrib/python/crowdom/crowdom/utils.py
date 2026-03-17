import decimal
import json
from functools import reduce
import logging
import threading
import time
from typing import List, Optional

import toloka.client as toloka

logger = logging.getLogger(__name__)


def get_pool_link(pool: toloka.Pool, client: toloka.TolokaClient) -> str:
    url_prefix = client.url[: -len('/api')]
    return f'{url_prefix}/requester/project/{pool.project_id}/pool/{pool.id}'


def wait_pool_for_close(
    client: toloka.TolokaClient,
    pool_id: str,
    pull_interval_seconds: float = 60.0,
):
    pool = client.get_pool(pool_id)
    while not pool.is_closed():
        logger.debug(f'waiting pool {get_pool_link(pool, client)} for close...')
        time.sleep(pull_interval_seconds)
        pool = client.get_pool(pool.id)


# 'reduce' is used to avoid redundant AND/OR's with only one element
def and_(filters: List[Optional[toloka.filter.FilterCondition]]) -> Optional[toloka.filter.FilterCondition]:
    filters = [f for f in filters if f is not None]
    if not filters:
        return None
    return reduce(lambda x, y: x & y, filters)


# 'reduce' is used to avoid redundant AND/OR's with only one element
def or_(filters: List[Optional[toloka.filter.FilterCondition]]) -> Optional[toloka.filter.FilterCondition]:
    filters = [f for f in filters if f is not None]
    if not filters:
        return None
    return reduce(lambda x, y: x | y, filters)


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)
