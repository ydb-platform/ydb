class CalculusMethods:
    pass

def defun(f):
    setattr(CalculusMethods, f.__name__, f)
    return f
