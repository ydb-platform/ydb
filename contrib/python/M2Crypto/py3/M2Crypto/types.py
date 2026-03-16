from typing import NewType

BIO = NewType('BIO', bytes)
BIO_METHOD = NewType('BIO_METHOD', bytes)
ASN1_Object = NewType('ASN1_Object', bytes)
ASN1_String = NewType('ASN1_String', bytes)
ASN1_Integer = NewType('ASN1_Integer', int)
ASN1_Time = NewType('ASN1_Time', bytes)
EC = NewType('EC', bytes)
X509 = NewType('X509', bytes)
X509_CRL = NewType('X509_CRL', bytes)
X509_STORE_CTX = NewType('X509_STORE_CTX', bytes)
