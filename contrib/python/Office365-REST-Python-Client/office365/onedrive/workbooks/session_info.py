from office365.runtime.client_value import ClientValue


class WorkbookSessionInfo(ClientValue):
    """Provides information about workbook session."""

    def __init__(self, session_id=None, persist_changes=None):
        """
        :param str session_id: Id of the workbook session.
        :param bool persist_changes: rue for persistent session. false for non-persistent session (view mode)
        """
        self.persistChanges = persist_changes
        self.id = session_id
