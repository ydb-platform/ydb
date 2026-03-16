import datetime


def camel_to_underscore(name):
    '''Convert camel case style naming to underscore style naming

    If there are existing underscores they will be collapsed with the
    to-be-added underscores. Multiple consecutive capital letters will not be
    split except for the last one.

    >>> camel_to_underscore('SpamEggsAndBacon')
    'spam_eggs_and_bacon'
    >>> camel_to_underscore('Spam_and_bacon')
    'spam_and_bacon'
    >>> camel_to_underscore('Spam_And_Bacon')
    'spam_and_bacon'
    >>> camel_to_underscore('__SpamAndBacon__')
    '__spam_and_bacon__'
    >>> camel_to_underscore('__SpamANDBacon__')
    '__spam_and_bacon__'
    '''
    output = []
    for i, c in enumerate(name):
        if i > 0:
            pc = name[i - 1]
            if c.isupper() and not pc.isupper() and pc != '_':
                # Uppercase and the previous character isn't upper/underscore?
                # Add the underscore
                output.append('_')
            elif i > 3 and not c.isupper():
                # Will return the last 3 letters to check if we are changing
                # case
                previous = name[i - 3:i]
                if previous.isalpha() and previous.isupper():
                    output.insert(len(output) - 1, '_')

        output.append(c.lower())

    return ''.join(output)


def timesince(dt, default='just now'):
    '''
    Returns string representing 'time since' e.g.
    3 days ago, 5 hours ago etc.

    >>> now = datetime.datetime.now()
    >>> timesince(now)
    'just now'
    >>> timesince(now - datetime.timedelta(seconds=1))
    '1 second ago'
    >>> timesince(now - datetime.timedelta(seconds=2))
    '2 seconds ago'
    >>> timesince(now - datetime.timedelta(seconds=60))
    '1 minute ago'
    >>> timesince(now - datetime.timedelta(seconds=61))
    '1 minute and 1 second ago'
    >>> timesince(now - datetime.timedelta(seconds=62))
    '1 minute and 2 seconds ago'
    >>> timesince(now - datetime.timedelta(seconds=120))
    '2 minutes ago'
    >>> timesince(now - datetime.timedelta(seconds=121))
    '2 minutes and 1 second ago'
    >>> timesince(now - datetime.timedelta(seconds=122))
    '2 minutes and 2 seconds ago'
    >>> timesince(now - datetime.timedelta(seconds=3599))
    '59 minutes and 59 seconds ago'
    >>> timesince(now - datetime.timedelta(seconds=3600))
    '1 hour ago'
    >>> timesince(now - datetime.timedelta(seconds=3601))
    '1 hour and 1 second ago'
    >>> timesince(now - datetime.timedelta(seconds=3602))
    '1 hour and 2 seconds ago'
    >>> timesince(now - datetime.timedelta(seconds=3660))
    '1 hour and 1 minute ago'
    >>> timesince(now - datetime.timedelta(seconds=3661))
    '1 hour and 1 minute ago'
    >>> timesince(now - datetime.timedelta(seconds=3720))
    '1 hour and 2 minutes ago'
    >>> timesince(now - datetime.timedelta(seconds=3721))
    '1 hour and 2 minutes ago'
    >>> timesince(datetime.timedelta(seconds=3721))
    '1 hour and 2 minutes ago'
    '''
    if isinstance(dt, datetime.timedelta):
        diff = dt
    else:
        now = datetime.datetime.now()
        diff = abs(now - dt)

    periods = (
        (diff.days / 365, 'year', 'years'),
        (diff.days % 365 / 30, 'month', 'months'),
        (diff.days % 30 / 7, 'week', 'weeks'),
        (diff.days % 7, 'day', 'days'),
        (diff.seconds / 3600, 'hour', 'hours'),
        (diff.seconds % 3600 / 60, 'minute', 'minutes'),
        (diff.seconds % 60, 'second', 'seconds'),
    )

    output = []
    for period, singular, plural in periods:
        if int(period):
            if int(period) == 1:
                output.append('%d %s' % (period, singular))
            else:
                output.append('%d %s' % (period, plural))

    if output:
        return '%s ago' % ' and '.join(output[:2])

    return default

