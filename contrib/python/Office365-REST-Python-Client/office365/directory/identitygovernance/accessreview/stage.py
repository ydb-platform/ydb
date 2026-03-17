from office365.entity import Entity


class AccessReviewStage(Entity):
    """
    Represents a stage of a Microsoft Entra access review. If the parent accessReviewScheduleDefinition has defined
    the stageSettings property, the accessReviewInstance is comprised of up to three subsequent stages.
    Each stage may have a different set of reviewers who can act on the stage decisions, and settings determining
    which decisions pass from stage to stage.

    Every accessReviewStage contains a list of decision items for reviewers.
    There's only one decision per identity being reviewed.
    """
