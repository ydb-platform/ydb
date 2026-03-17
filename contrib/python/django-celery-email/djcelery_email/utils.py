import copy
import base64
from email.mime.base import MIMEBase

from django.conf import settings
from django.core.mail import EmailMultiAlternatives, EmailMessage


def chunked(iterator, chunksize):
    """
    Yields items from 'iterator' in chunks of size 'chunksize'.

    >>> list(chunked([1, 2, 3, 4, 5], chunksize=2))
    [(1, 2), (3, 4), (5,)]
    """
    chunk = []
    for idx, item in enumerate(iterator, 1):
        chunk.append(item)
        if idx % chunksize == 0:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def email_to_dict(message):
    if isinstance(message, dict):
        return message

    message_dict = {'subject': message.subject,
                    'body': message.body,
                    'from_email': message.from_email,
                    'to': message.to,
                    'bcc': message.bcc,
                    # ignore connection
                    'attachments': [],
                    'headers': message.extra_headers,
                    'cc': message.cc,
                    'reply_to': message.reply_to}

    if hasattr(message, 'alternatives'):
        message_dict['alternatives'] = message.alternatives
    if message.content_subtype != EmailMessage.content_subtype:
        message_dict["content_subtype"] = message.content_subtype
    if message.mixed_subtype != EmailMessage.mixed_subtype:
        message_dict["mixed_subtype"] = message.mixed_subtype

    attachments = message.attachments
    for attachment in attachments:
        if isinstance(attachment, MIMEBase):
            filename = attachment.get_filename('')
            binary_contents = attachment.get_payload(decode=True)
            mimetype = attachment.get_content_type()
        else:
            filename, binary_contents, mimetype = attachment
            # For a mimetype starting with text/, content is expected to be a string.
            if isinstance(binary_contents, str):
                binary_contents = binary_contents.encode()
        contents = base64.b64encode(binary_contents).decode('ascii')
        message_dict['attachments'].append((filename, contents, mimetype))

    if settings.CELERY_EMAIL_MESSAGE_EXTRA_ATTRIBUTES:
        for attr in settings.CELERY_EMAIL_MESSAGE_EXTRA_ATTRIBUTES:
            if hasattr(message, attr):
                message_dict[attr] = getattr(message, attr)

    return message_dict


def dict_to_email(messagedict):
    message_kwargs = copy.deepcopy(messagedict)  # prevents missing items on retry

    # remove items from message_kwargs until only valid EmailMessage/EmailMultiAlternatives kwargs are left
    # and save the removed items to be used as EmailMessage/EmailMultiAlternatives attributes later
    message_attributes = ['content_subtype', 'mixed_subtype']
    if settings.CELERY_EMAIL_MESSAGE_EXTRA_ATTRIBUTES:
        message_attributes.extend(settings.CELERY_EMAIL_MESSAGE_EXTRA_ATTRIBUTES)
    attributes_to_copy = {}
    for attr in message_attributes:
        if attr in message_kwargs:
            attributes_to_copy[attr] = message_kwargs.pop(attr)

    # remove attachments from message_kwargs then reinsert after base64 decoding
    attachments = message_kwargs.pop('attachments')
    message_kwargs['attachments'] = []
    for attachment in attachments:
        filename, contents, mimetype = attachment
        contents = base64.b64decode(contents.encode('ascii'))

        # For a mimetype starting with text/, content is expected to be a string.
        if mimetype and mimetype.startswith('text/'):
            contents = contents.decode()

        message_kwargs['attachments'].append((filename, contents, mimetype))

    if 'alternatives' in message_kwargs:
        message = EmailMultiAlternatives(**message_kwargs)
    else:
        message = EmailMessage(**message_kwargs)

    # set attributes on message with items removed from message_kwargs earlier
    for attr, val in attributes_to_copy.items():
        setattr(message, attr, val)

    return message
