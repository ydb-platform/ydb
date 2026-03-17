
traditional = [
    (1024 ** 5, 'P'),
    (1024 ** 4, 'T'), 
    (1024 ** 3, 'G'), 
    (1024 ** 2, 'M'), 
    (1024 ** 1, 'K'),
    (1024 ** 0, 'B'),
    ]

alternative = [
    (1024 ** 5, ' PB'),
    (1024 ** 4, ' TB'), 
    (1024 ** 3, ' GB'), 
    (1024 ** 2, ' MB'), 
    (1024 ** 1, ' KB'),
    (1024 ** 0, (' byte', ' bytes')),
    ]

verbose = [
    (1024 ** 5, (' petabyte', ' petabytes')),
    (1024 ** 4, (' terabyte', ' terabytes')), 
    (1024 ** 3, (' gigabyte', ' gigabytes')), 
    (1024 ** 2, (' megabyte', ' megabytes')), 
    (1024 ** 1, (' kilobyte', ' kilobytes')),
    (1024 ** 0, (' byte', ' bytes')),
    ]

iec = [
    (1024 ** 5, 'Pi'),
    (1024 ** 4, 'Ti'),
    (1024 ** 3, 'Gi'), 
    (1024 ** 2, 'Mi'), 
    (1024 ** 1, 'Ki'),
    (1024 ** 0, ''),
    ]

si = [
    (1000 ** 5, 'P'),
    (1000 ** 4, 'T'), 
    (1000 ** 3, 'G'), 
    (1000 ** 2, 'M'), 
    (1000 ** 1, 'K'),
    (1000 ** 0, 'B'),
    ]



def size(bytes, system=traditional):
    """Human-readable file size.

    Using the traditional system, where a factor of 1024 is used::
    
    >>> size(10)
    '10B'
    >>> size(100)
    '100B'
    >>> size(1000)
    '1000B'
    >>> size(2000)
    '1K'
    >>> size(10000)
    '9K'
    >>> size(20000)
    '19K'
    >>> size(100000)
    '97K'
    >>> size(200000)
    '195K'
    >>> size(1000000)
    '976K'
    >>> size(2000000)
    '1M'
    
    Using the SI system, with a factor 1000::

    >>> size(10, system=si)
    '10B'
    >>> size(100, system=si)
    '100B'
    >>> size(1000, system=si)
    '1K'
    >>> size(2000, system=si)
    '2K'
    >>> size(10000, system=si)
    '10K'
    >>> size(20000, system=si)
    '20K'
    >>> size(100000, system=si)
    '100K'
    >>> size(200000, system=si)
    '200K'
    >>> size(1000000, system=si)
    '1M'
    >>> size(2000000, system=si)
    '2M'
    
    """
    for factor, suffix in system:
        if bytes >= factor:
            break
    amount = int(bytes/factor)
    if isinstance(suffix, tuple):
        singular, multiple = suffix
        if amount == 1:
            suffix = singular
        else:
            suffix = multiple
    return str(amount) + suffix

