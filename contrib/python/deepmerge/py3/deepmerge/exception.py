class DeepMergeException(Exception):
    pass


class StrategyNotFound(DeepMergeException):
    pass


class InvalidMerge(DeepMergeException):
    def __init__(self, strategy_list_name, merge_args, merge_kwargs):
        super(InvalidMerge, self).__init__(
            "no more strategies found for {0} and arguments {1}, {2}".format(
                strategy_list_name, merge_args, merge_kwargs
            )
        )
        self.strategy_list_name = strategy_list_name
        self.merge_args = merge_args
        self.merge_kwargs = merge_kwargs
