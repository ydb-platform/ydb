#'s' b''
#u's' b'ä'

result = u's'.encode('utf-8') + b'ä'
print(result)  # out: 's\xc3\xa4'

result = u's' + b'ä'.decode('utf-8')
print(result)  # out: u'sä'
