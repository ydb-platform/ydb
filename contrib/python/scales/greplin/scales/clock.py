import time

class BasicClock(object):
  """ Abstraction between things that use sources of time and the rest of the system
      this allows for independent clocks to be integrated with potentially different
      levels of granularity """

  def time(self):
    return time.time()

def getClock():
    """ Returns the best, most accurate clock possible """
    return BasicClock()
