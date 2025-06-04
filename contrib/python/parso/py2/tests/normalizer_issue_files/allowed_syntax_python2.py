#'s' b''
#u's' b'ä'

result = u's'.encode('utf-8') + b'\xc3\xa4'
print(result)  # out: b's\xc3\xa4'

result = u's' + b'\xc3\xa4'.decode('utf-8')
print(result)  # out: u'sä'
