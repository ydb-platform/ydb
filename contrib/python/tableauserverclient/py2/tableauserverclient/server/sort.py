class Sort(object):
    def __init__(self, field, direction):
        self.field = field
        self.direction = direction

    def __str__(self):
        return '{0}:{1}'.format(self.field, self.direction)
