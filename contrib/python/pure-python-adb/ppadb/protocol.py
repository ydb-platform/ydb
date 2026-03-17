class Protocol:
    OKAY = 'OKAY'
    FAIL = 'FAIL'
    STAT = 'STAT'
    LIST = 'LIST'
    DENT = 'DENT'
    RECV = 'RECV'
    DATA = 'DATA'
    DONE = 'DONE'
    SEND = 'SEND'
    QUIT = 'QUIT'

    @staticmethod
    def decode_length(length):
        return int(length, 16)

    @staticmethod
    def encode_length(length):
        return "{0:04X}".format(length)

    @staticmethod
    def encode_data(data):
        b_data = data.encode('utf-8')
        b_length = Protocol.encode_length(len(b_data)).encode('utf-8')
        return b"".join([b_length, b_data])
