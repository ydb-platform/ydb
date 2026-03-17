from typing import Optional

from qdrant_client.http import models


DEFAULT_RANKING_CONSTANT_K = 2


def reciprocal_rank_fusion(
    responses: list[list[models.ScoredPoint]],
    limit: int = 10,
    ranking_constant_k: Optional[int] = None,
) -> list[models.ScoredPoint]:
    def compute_score(pos: int) -> float:
        ranking_constant = (
            ranking_constant_k if ranking_constant_k is not None else DEFAULT_RANKING_CONSTANT_K
        )  # mitigates the impact of high rankings by outlier systems
        return 1 / (ranking_constant + pos)

    scores: dict[models.ExtendedPointId, float] = {}
    point_pile = {}
    for response in responses:
        for i, scored_point in enumerate(response):
            if scored_point.id in scores:
                scores[scored_point.id] += compute_score(i)
            else:
                point_pile[scored_point.id] = scored_point
                scores[scored_point.id] = compute_score(i)

    sorted_scores = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    sorted_points = []
    for point_id, score in sorted_scores[:limit]:
        point = point_pile[point_id]
        point.score = score
        sorted_points.append(point)
    return sorted_points


def distribution_based_score_fusion(
    responses: list[list[models.ScoredPoint]], limit: int
) -> list[models.ScoredPoint]:
    def normalize(response: list[models.ScoredPoint]) -> list[models.ScoredPoint]:
        if len(response) == 1:
            response[0].score = 0.5
            return response

        total = sum([point.score for point in response])
        mean = total / len(response)
        variance = sum([(point.score - mean) ** 2 for point in response]) / (len(response) - 1)

        if variance == 0:
            for point in response:
                point.score = 0.5
            return response

        std_dev = variance**0.5
        low = mean - 3 * std_dev
        high = mean + 3 * std_dev

        for point in response:
            point.score = (point.score - low) / (high - low)

        return response

    points_map: dict[models.ExtendedPointId, models.ScoredPoint] = {}
    for response in responses:
        if not response:
            continue
        normalized = normalize(response)
        for point in normalized:
            entry = points_map.get(point.id)
            if entry is None:
                points_map[point.id] = point
            else:
                entry.score += point.score

    sorted_points = sorted(points_map.values(), key=lambda item: item.score, reverse=True)

    return sorted_points[:limit]
