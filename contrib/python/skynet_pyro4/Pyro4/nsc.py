"""
Name server control tool.

Pyro - Python Remote Objects.  Copyright by Irmen de Jong.
irmen@razorvine.net - http://www.razorvine.net/projects/Pyro
"""

import sys
from Pyro4 import naming, errors

if sys.version_info<(3, 0):
    input=raw_input


def handleCommand(nameserver, options, args):
    def printListResult(resultdict, title=""):
        print("--------START LIST %s" % title)
        for name, uri in sorted(resultdict.items()):
            print("%s --> %s" % (name, uri))
        print("--------END LIST %s" % title)

    def cmd_ping():
        nameserver.ping()
        print("Name server ping ok.")

    def cmd_listprefix():
        if len(args)==1:
            printListResult(nameserver.list())
        else:
            printListResult(nameserver.list(prefix=args[1]), "- prefix '%s'" % args[1])

    def cmd_listregex():
        if len(args)<2:
            raise SystemExit("missing regex argument")
        printListResult(nameserver.list(regex=args[1]), "- regex '%s'" % args[1])

    def cmd_register():
        nameserver.register(args[1], args[2], safe=True)
        print("Registered %s" % args[1])

    def cmd_remove():
        count=nameserver.remove(args[1])
        if count>0:
            print("Removed %s" % args[1])
        else:
            print("Nothing removed")

    def cmd_removeregex():
        if len(args)<2:
            raise SystemExit("missing regex argument")
        sure=input("Potentially removing lots of items from the Name server. Are you sure (y/n)?").strip()
        if sure in ('y', 'Y'):
            count=nameserver.remove(regex=args[1])
            print("%d items removed." % count)

    commands={
        "ping": cmd_ping,
        "list": cmd_listprefix,
        "listmatching": cmd_listregex,
        "register": cmd_register,
        "remove": cmd_remove,
        "removematching": cmd_removeregex
    }
    try:
        commands[args[0]]()
    except Exception:
        x=sys.exc_info()[1]
        print("Error: %s" % repr(x))


def main(args):
    from optparse import OptionParser
    usage = "usage: %prog [options] command [arguments]\nCommand is one of: " \
            "register remove removematching list listmatching ping"
    parser = OptionParser(usage=usage)
    parser.add_option("-n", "--host", dest="host", help="hostname of the NS")
    parser.add_option("-p", "--port", dest="port", type="int",
                      help="port of the NS (or bc-port if host isn't specified)")
    parser.add_option("-u","--unixsocket", help="unix domain socket name of the NS")
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose", help="verbose output")
    options, args = parser.parse_args(args)
    if not args or args[0] not in ("register", "remove", "removematching", "list", "listmatching", "ping"):
        parser.error("invalid or missing command")
    if options.verbose:
        print("Locating name server...")
    if options.unixsocket:
        options.host="./u:"+options.unixsocket
    try:

        nameserver=naming.locateNS(options.host, options.port)
    except errors.PyroError:
        x=sys.exc_info()[1]
        print("Failed to locate the name server: %s" % x)
        return
    if options.verbose:
        print("Name server found: %s" % nameserver._pyroUri)
    handleCommand(nameserver, options, args)
    if options.verbose:
        print("Done.")

if __name__=="__main__":
    main(sys.argv[1:])
