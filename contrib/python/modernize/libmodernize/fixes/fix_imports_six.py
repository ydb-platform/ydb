from __future__ import generator_stop

from fissix.fixes import fix_imports


class FixImportsSix(fix_imports.FixImports):

    mapping = {
        "__builtin__": "six.moves.builtins",
        "_winreg": "six.moves.winreg",
        "BaseHTTPServer": "six.moves.BaseHTTPServer",
        "CGIHTTPServer": "six.moves.CGIHTTPServer",
        "ConfigParser": "six.moves.configparser",
        "copy_reg": "six.moves.copyreg",
        "Cookie": "six.moves.http_cookies",
        "cookielib": "six.moves.http_cookiejar",
        "cPickle": "six.moves.cPickle",
        "Dialog": "six.moves.tkinter_dialog",
        "dummy_thread": "six.moves._dummy_thread",
        # cStringIO.StringIO()
        # email.MIMEBase
        # email.MIMEMultipart
        # email.MIMENonMultipart
        # email.MIMEText
        "FileDialog": "six.moves.tkinter_filedialog",
        "gdbm": "six.moves.dbm_gnu",
        "htmlentitydefs": "six.moves.html_entities",
        "HTMLParser": "six.moves.html_parser",
        "httplib": "six.moves.http_client",
        # intern()
        # itertools.ifilter()
        # itertools.ifilterfalse()
        # itertools.imap()
        # itertools.izip()
        # itertools.zip_longest()
        # pipes.quote
        "Queue": "six.moves.queue",
        # reduce()
        # reload()
        "repr": "six.moves.reprlib",
        "robotparser": "six.moves.urllib_robotparser",
        "ScrolledText": "six.moves.tkinter_scrolledtext",
        "SimpleDialog": "six.moves.tkinter_simpledialog",
        "SimpleHTTPServer": "six.moves.SimpleHTTPServer",
        "SimpleXMLRPCServer": "six.moves.xmlrpc_server",
        "SocketServer": "six.moves.socketserver",
        "thread": "six.moves._thread",
        "Tix": "six.moves.tkinter_tix",
        "tkColorChooser": "six.moves.tkinter_colorchooser",
        "tkCommonDialog": "six.moves.tkinter_commondialog",
        "Tkconstants": "six.moves.tkinter_constants",
        "Tkdnd": "six.moves.tkinter_dnd",
        "tkFileDialog": "six.moves.tkinter_filedialog",
        "tkFont": "six.moves.tkinter_font",
        "Tkinter": "six.moves.tkinter",
        "tkMessageBox": "six.moves.tkinter_messagebox",
        "tkSimpleDialog": "six.moves.tkinter_tksimpledialog",
        "ttk": "six.moves.tkinter_ttk",
        # urllib
        "urlparse": "six.moves.urllib.parse",
        # UserDict.UserDict
        # UserList.UserList
        # UserString.UserString
        "xmlrpclib": "six.moves.xmlrpc_client",
    }
