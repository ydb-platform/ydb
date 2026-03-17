class Channel(object):
    '''
    A Channel represents a public or private Slack Channel instance
    '''
    def __init__(self, server, name, channel_id, members=None):
        self.server = server
        self.name = name
        self.id = channel_id
        self.members = [] if members is None else members

    def __eq__(self, compare_str):
        if self.name == compare_str or "#" + self.name == compare_str or self.id == compare_str:
            return True
        else:
            return False

    def __hash__(self):
        return hash(self.id)

    def __str__(self):
        data = ""
        for key in list(self.__dict__.keys()):
            data += "{0} : {1}\n".format(key, str(self.__dict__[key])[:40])
        return data

    def __repr__(self):
        return self.__str__()

    def send_message(self, message, thread=None, reply_broadcast=False):
        '''
        Sends a message to a this Channel.

        Include the parent message's thread_ts value in `thread`
        to send to a thread.

        :Args:
            message (message) - the string you'd like to send to the channel
            thread (str or None) - the parent message ID, if sending to a
                thread
            reply_broadcast (bool) - if messaging a thread, whether to
                also send the message back to the channel

        :Returns:
            None
        '''
        self.server.rtm_send_message(self.id, message, thread, reply_broadcast)
