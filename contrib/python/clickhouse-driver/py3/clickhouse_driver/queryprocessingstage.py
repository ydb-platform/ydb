
class QueryProcessingStage(object):
    """
    Determines till which state SELECT query should be executed.
    """
    FETCH_COLUMNS = 0
    WITH_MERGEABLE_STATE = 1
    COMPLETE = 2
