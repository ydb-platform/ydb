from office365.runtime.client_value import ClientValue


class MoveCopyOptions(ClientValue):
    """"""

    def __init__(
        self,
        keep_both=True,
        reset_author_and_created_on_copy=False,
        retain_editor_and_modified_on_move=False,
        should_bypass_shared_locks=False,
    ):
        super(MoveCopyOptions, self).__init__()
        """
        :param bool retain_editor_and_modified_on_move: Specifies whether to retain the source of the move's editor
            and modified by datetime
        """
        self.KeepBoth = keep_both
        self.ResetAuthorAndCreatedOnCopy = reset_author_and_created_on_copy
        self.RetainEditorAndModifiedOnMove = retain_editor_and_modified_on_move
        self.ShouldBypassSharedLocks = should_bypass_shared_locks

    @property
    def entity_type_name(self):
        return "SP.MoveCopyOptions"
