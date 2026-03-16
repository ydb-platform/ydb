SQS_XRAY_HEADER = "AWSTraceHeader"
class SqsMessageHelper:
    
    @staticmethod 
    def isSampled(sqs_message):
        attributes = sqs_message['attributes']

        if SQS_XRAY_HEADER not in attributes:
            return False

        return 'Sampled=1' in attributes[SQS_XRAY_HEADER]