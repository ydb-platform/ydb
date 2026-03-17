from typing import Optional

from motor.motor_asyncio import AsyncIOMotorClientSession


class SessionMethods:
    """
    Session methods
    """

    def set_session(self, session: Optional[AsyncIOMotorClientSession] = None):
        """
        Set motor session
        :param session: Optional[AsyncIOMotorClientSession] - motor session
        :return:
        """
        if session is not None:
            self.session: Optional[AsyncIOMotorClientSession] = session
        return self
