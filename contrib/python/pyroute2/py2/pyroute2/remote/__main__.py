import sys
from pyroute2.remote import Server
from pyroute2.remote import Transport


Server(Transport(sys.stdin), Transport(sys.stdout))
