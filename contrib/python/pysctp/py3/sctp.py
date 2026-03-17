# SCTP bindings for Python
# -*- coding: utf-8 -*-
#
# Python-side bindings
# 
# This library is free software; you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by the
# Free Software Foundation; either version 2.1 of the License, or (at your
# option) any later version.
#
# This library is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this library; If not, see <http://www.gnu.org/licenses/>.
# 
# Elvis Pfützenreuter (elvis.pfutzenreuter@{gmail.com,indt.org.br})
# Copyright (c) 2005 Instituto Nokia de Tecnologia

"""
Python bindings for SCTP. It is compatible with kernel-level SCTP
implementations with BSD/Sockets API.

For the users, the relevant classes and functions are:

features()
sctpsocket(): base class, ought not used directly, althrough it can be
sctpsocket_tcp(): TCP-style subclass
sctpsocket_udp(): UDP-style subclass

SCTP sockets do NOT inherit from socket._socketobject, instead they
CONTAIN a standard Python socket, and DELEGATE unknown calls to it.
So it should fit in most places where a "real" socket is expected.

When receiving data, it is also possible to receive metadata in the
form of events. An event will be one of the following classes:

sndrcvinfo(): 
notification(): 
assoc_change():
paddr_change(): 
send_failed(): 
remote_error(): 
shutdown_event(): 
pdapi_event(): 
adaptation_event(): 

Every SCTP socket has a number of properties. Two "complex" properties,
that contain a number of sub-properties, are: 

sctpsocket.events = event_subscribe()
sctpsocket.initparams = initparams()

Please take a look in the classes' documentation to learn more about
respective sub-properties.

Most SCTP-specific macros are available as constants in Python, so 
an experienced SCTP/C API programmer will feel at home at pysctp.
The only difference is that constants used only inside a particular
event, are defined as class constants and not as module constants,
to avoid excessive namespace pollution.

"""

from __future__ import print_function
from socket     import *
import socket
import _sctp

import datetime
import time
import os
import sys

####################################### CONSTANTS

# bindx() constants
BINDX_ADD = _sctp.getconstant("BINDX_ADD")
BINDX_REMOVE = _sctp.getconstant("BINDX_REMOVE")
BINDX_REM = BINDX_REMOVE

# high-level (sndrcvinfo) message flags
MSG_UNORDERED = _sctp.getconstant("MSG_UNORDERED")
MSG_ADDR_OVER = _sctp.getconstant("MSG_ADDR_OVER")
MSG_ABORT = _sctp.getconstant("MSG_ABORT")
MSG_EOF = _sctp.getconstant("MSG_EOF")
MSG_FIN = _sctp.getconstant("MSG_FIN")
MSG_SENDALL = _sctp.getconstant("MSG_SENDALL")
MSG_ADDR_OVERRIDE = MSG_ADDR_OVER

# low-level (sendto/sendmsg) flags
FLAG_NOTIFICATION = _sctp.getconstant("MSG_NOTIFICATION")
FLAG_EOR = _sctp.getconstant("MSG_EOR")
FLAG_DONTROUTE = _sctp.getconstant("MSG_DONTROUTE")

(HAVE_SCTP, HAVE_KERNEL_SCTP, HAVE_SCTP_MULTIBUF, HAVE_SCTP_NOCONNECT, \
 HAVE_SCTP_PRSCTP, HAVE_SCTP_ADDIP, HAVE_SCTP_CANSET_PRIMARY, HAVE_SCTP_SAT_NETWORK_CAPABILITY) = \
 (1,2,4,8,16,32,64,128)

# socket constants
SOL_SCTP = _sctp.getconstant("SOL_SCTP")
IPPROTO_SCTP = _sctp.getconstant("IPPROTO_SCTP")
SOCK_SEQPACKET = _sctp.getconstant("SOCK_SEQPACKET")
SOCK_STREAM = _sctp.getconstant("SOCK_STREAM")
(STYLE_TCP, STYLE_UDP) = (SOCK_STREAM, SOCK_SEQPACKET)
(TCP_STYLE, UDP_STYLE) = (STYLE_TCP, STYLE_UDP)


####################################### STRUCTURES FOR SCTP MESSAGES AND EVENTS

class initmsg(object):
	"""
	Object used in explicit opening of SCTP associations in
	UDP-style sockets without passing any user-data message
	(UNIMPLEMENTED in pysctp, probably needs direct sendmsg() use)

	User will be better off it instantiate this class via
	initparams.initmsg() constructor, because it fills initmsg with
	socket's defaults.
	"""
	def __init__(self):
		self.num_ostreams = 0
		self.max_instreams = 0
		self.max_attempts = 0
		self.max_init_timeo = 0

class initparams(object):
	"""
	SCTP default initialization parameters. New associations are opened
	with the properties specified in this object.

	Properties are: 
	
	num_ostreams
	max_instreams
	max_attempts
	max_init_timeo (in seconds)
	
	Setting of properties will automatically call flush() (that will 
	setsockopt the socket) unless "autoflush" is set to False.

	User should never need to instantiate this class directly. For every
	property, there is a pair of get_P and set_P methods, that user can,
	but should not need, to call directly.
	"""

	def __init__(self, container):
		"""
		The container object passed as parameter will (I hope) be a 
		sctpsocket() descendant. It should offer the _get_initparams() 
		and _set_initparams() for the socket it represents.
		"""
		self.autoflush = True
		self.container = container
		self._num_ostreams = self._max_instreams = 0
		self._max_attempts = self._max_init_timeo = 0
		self.__dict__.update(self.container._get_initparams())

	def flush(self):
		"""
		Flushes the initialization properties to the socket
		(setsockopt). If autoflush equals True (the default),
		any change to properties will call this automatically.
		"""
		self.container._set_initparams(self.__dict__)

	def initmsg(self):
		"""
		Builds a initmsg() object containing the same properties
		as the socket initalization parameters. The initmsg() 
		can then be changed and used to open a particular SCTP
		association.
		"""
		r = initmsg()
		r.num_ostreams = self._num_ostreams;
		r.max_instreams = self._max_instreams;
		r.max_attempts = self._max_attempts;
		r.max_init_timeo = self._max_init_timeo;
		return r
	
	def get_num_ostreams(self):
		return self._num_ostreams;

	def set_num_ostreams(self, v):
		self._num_ostreams = v;
		if self.autoflush:
			self.flush()
		
	def get_max_instreams(self):
		return self._max_instreams;

	def set_max_instreams(self, v):
		self._max_instreams = v;
		if self.autoflush:
			self.flush()
		
	def get_max_attempts(self):
		return self._max_attempts;

	def set_max_attempts(self, v):
		self._max_attempts = v;
		if self.autoflush:
			self.flush()
		
	def get_max_init_timeo(self):
		return self._max_init_timeo;

	def set_max_init_timeo(self, v):
		self._max_init_timeo = v;
		if self.autoflush:
			self.flush()
		
	num_ostreams = property(get_num_ostreams, set_num_ostreams)
	max_instreams = property(get_max_instreams, set_max_instreams)
	max_attempts = property(get_max_attempts, set_max_attempts)
	max_init_timeo = property(get_max_init_timeo, set_max_init_timeo)

class sndrcvinfo(object):
	"""
	Send/receive ancilliary data. In PySCTP, this object is only used
	to *receive* ancilliary information about a message. The user
	should never need to instantiate this class.

	The "flag" attribute is a bitmap of the MSG_* class constants.
	"""

	def __init__(self, values=None):
		self.stream = 0
		self.ssn = 0
		self.flags = 0
		self.context = 0
		self.timetolive = 0
		self.tsn = 0
		self.cumtsn = 0
		self.assoc_id = 0
		if values:
			self.__dict__.update(values)

class notification(object):
	"""
	Base class for all notification objects. Objects of this particular
	type should never appear in message receiving. If it does, it means
	that PySCTP received a notification it does not understand (probably
	a "new" event type), and author must be warned about this.

	The "type" and "flags" fields belong to this class because they
	appear in every notification. This class contains "type_*" constants
	that identify every notification type. On the other hand, "flags"
	meanings are particular for every notification type.

	The user should never need to instantiate this class.
	"""
	def __init__(self, values):
		self.type = 0
		self.flags = 0
		if values:
			self.__dict__.update(values)

	type_SN_TYPE_BASE = _sctp.getconstant("SCTP_SN_TYPE_BASE")
	type_ASSOC_CHANGE = _sctp.getconstant("SCTP_ASSOC_CHANGE")
	type_PEER_ADDR_CHANGE = _sctp.getconstant("SCTP_PEER_ADDR_CHANGE")
	type_SEND_FAILED = _sctp.getconstant("SCTP_SEND_FAILED")
	type_REMOTE_ERROR = _sctp.getconstant("SCTP_REMOTE_ERROR")
	type_SHUTDOWN_EVENT = _sctp.getconstant("SCTP_SHUTDOWN_EVENT")
	type_PARTIAL_DELIVERY_EVENT = _sctp.getconstant("SCTP_PARTIAL_DELIVERY_EVENT")
	type_ADAPTATION_INDICATION = _sctp.getconstant("SCTP_ADAPTATION_INDICATION")

class assoc_change(notification):
	"""
	Association change notification. The relevant values are:
	
	state
	error
	outbound_streams
	inbound_streams
	assoc_id

	It seems to lack remote address/port pair, but remember that recvmsg() and
	sctp_recv() return the address for every (either data or event) message, so
	the application can have full knowledge about the new connection.

	Most UDP-style socket users will want to save this event, in particular "assoc_id"
	because that value will be needed to further refer to the association. Address/pair
	port can also be used to identify the remote peer, but it can be ambiguous since
	SCTP can make multihomed associations.

	The "state" can be tested against "state_*" class constants.  If state is 
	state_CANT_START_ASSOCIATION, "error" will be one of the "error_*" constants.

	The user should never need to instantiate this directly. This notification will 
	be received only if user subscribed to receive that (see event_subscribe class 
	for details).
	"""
	def __init__(self, values=None):
		self.state = 0
		self.error = 0
		self.outbound_streams = 0
		self.inbound_streams = 0
		self.assoc_id = 0
		notification.__init__(self, values)

	state_COMM_UP = _sctp.getconstant("SCTP_COMM_UP")
	state_COMM_LOST = _sctp.getconstant("SCTP_COMM_LOST")
	state_RESTART = _sctp.getconstant("SCTP_RESTART")
	state_SHUTDOWN_COMP = _sctp.getconstant("SCTP_SHUTDOWN_COMP")
	state_CANT_STR_ASSOC = _sctp.getconstant("SCTP_CANT_STR_ASSOC")
	state_CANT_START_ASSOCIATION = _sctp.getconstant("SCTP_CANT_STR_ASSOC")

	error_FAILED_THRESHOLD = _sctp.getconstant("SCTP_FAILED_THRESHOLD")
	error_RECEIVED_SACK = _sctp.getconstant("SCTP_RECEIVED_SACK")
	error_HEARTBEAT_SUCCESS = _sctp.getconstant("SCTP_HEARTBEAT_SUCCESS")
	error_RESPONSE_TO_USER_REQ = _sctp.getconstant("SCTP_RESPONSE_TO_USER_REQ")
	error_INTERNAL_ERROR = _sctp.getconstant("SCTP_INTERNAL_ERROR")
	error_SHUTDOWN_GUARD_EXPIRES = _sctp.getconstant("SCTP_SHUTDOWN_GUARD_EXPIRES")
	error_PEER_FAULTY = _sctp.getconstant("SCTP_PEER_FAULTY")

class paddr_change(notification):
	"""
	Peer address change notification. This event is received when a multihomed remote
	peer changes the network interface in use.

	The user should never need to instantiate this directly. This
	notification will be received only if user subscribed to receive
	that (see event_subscribe class for details).
	"""
	def __init__(self, values=None):
		self.addr = ("",0)
		self.state = 0
		self.error = 0
		self.assoc_id = 0
		notification.__init__(self, values)

	state_ADDR_AVAILABLE = _sctp.getconstant("SCTP_ADDR_AVAILABLE")
	state_ADDR_UNREACHABLE = _sctp.getconstant("SCTP_ADDR_UNREACHABLE")
	state_ADDR_REMOVED = _sctp.getconstant("SCTP_ADDR_REMOVED")
	state_ADDR_ADDED = _sctp.getconstant("SCTP_ADDR_ADDED")
	state_ADDR_MADE_PRIM = _sctp.getconstant("SCTP_ADDR_MADE_PRIM")

	error_FAILED_THRESHOLD = assoc_change.error_FAILED_THRESHOLD
	error_RECEIVED_SACK = assoc_change.error_RECEIVED_SACK
	error_HEARTBEAT_SUCCESS = assoc_change.error_HEARTBEAT_SUCCESS
	error_RESPONSE_TO_USER_REQ = assoc_change.error_RESPONSE_TO_USER_REQ
	error_INTERNAL_ERROR = assoc_change.error_INTERNAL_ERROR
	error_SHUTDOWN_GUARD_EXPIRES = assoc_change.error_SHUTDOWN_GUARD_EXPIRES
	error_PEER_FAULTY = assoc_change.error_PEER_FAULTY

class remote_error(notification):
	"""
	Remote error notification. This is received when the remote application
	explicitely sends a SCTP_REMOTE_ERROR notification. If the application
	protocol does not use this feature, such messages don't need to be
	handled.

	It will NOT be received when an "actual", or low-level, error occurs. 
	For such errors, assoc_change() objects are passed instead.

	The user should never need to instantiate this directly. This
	notification will be received only if user subscribed to receive
	that (see event_subscribe class for details).
	"""
	def __init__(self, values=None):
		self.error = 0
		self.assoc_id = 0
		self.data = ""
		notification.__init__(self, values)

class send_failed(notification):
	"""
	Send error notification. This is received when a particular message could not
	be sent.

	When an association fails, all pending messages will be returned as send_failed()s
	*after* assoc_change() notification is reveived. Also, send_failed() is returned 
	when the message time-to-live expires (which is not a fatal error).

	The "flag" attribute can have one of the flag_* class constants. More details about
	the message, including the association ID, can be obtained inside the "info"
	attribute, that is a sndrcvinfo() object. See sndrcvinfo class documentation for
	more details.
	
	The user should never need to instantiate this directly. This
	notification will be received only if user subscribed to receive
	that (see event_subscribe class for details).
	"""
	def __init__(self, values=None):
		self.error = 0
		self.assoc_id = 0
		self.data = ""
		
		notification.__init__(self, values)
		self.info = sndrcvinfo(self._info)
		del self._info

	flag_DATA_UNSENT = _sctp.getconstant("SCTP_DATA_UNSENT")
	flag_DATA_SENT = _sctp.getconstant("SCTP_DATA_SENT")

class shutdown_event(notification):
	"""
	Shutdown event. This event is received when an association goes
	down. The only relevant attribute is assoc_id.
	
	If user has subscribed to receive assoc_change notifications, it 
	will receive at least 2 messages (assoc_change and shutdown_event) 
	for an association shutdown. In addition, if the association was
	down by some problem, more error notifications will be received
	before this event.

	The user should never need to instantiate this directly. This
	notification will be received only if user subscribed to receive
	that (see event_subscribe class for details).
	"""
	def __init__(self, values=None):
		self.assoc_id = 0
		notification.__init__(self, values)
	
class adaptation_event(notification):
	"""
	Adaption indication event. It signals that the remote peer requests
	an specific adaptation layer.

	In SCTP, you can pass an optional 32-bit metadata identifier 
	along each message, called "ppid". For protocols that actively use
	this feature, it is like a subprotocol identifier, or "adaptation
	layer". The message interpreter is chosen by this value. This is
	mostly for telephony signaling-related protocols.

	When receiving this event, the new messages should be sent with
	the new requested ppid.

	For protocols that use multiple ppids, or do not take advantage of
	ppid at all, this notification is useless and can be ignored, or
	even better, unsubscribed.

	The user should never need to instantiate this directly. This
	notification will be received only if user subscribed to receive
	that (see event_subscribe class for details).
	"""
	def __init__(self, values=None):
		self.adaptation_ind = 0
		self.assoc_id = 0
		notification.__init__(self, values)

class pdapi_event(notification):
	"""
	Partial delivery event. This event is received when a partial
	delivery event occurs.

	The user should never need to instantiate this directly. This
	notification will be received only if user subscribed to receive
	that (see event_subscribe class for details).

	The indication can be one of the indication_* values.
	"""
	def __init__(self, values=None):
		self.indication = 0
		self.assoc_id = 0
		notification.__init__(self, values)

	indication_PD_ABORTED = _sctp.getconstant("SCTP_PARTIAL_DELIVERY_ABORTED")
	indication_PARTIAL_DELIVERY_ABORTED = indication_PD_ABORTED

#################################################### NOTIFICATION FACTORY

notification_table = {
	notification.type_ASSOC_CHANGE: assoc_change,
	notification.type_PEER_ADDR_CHANGE: paddr_change,
	notification.type_SEND_FAILED: send_failed,
	notification.type_REMOTE_ERROR: remote_error,
	notification.type_SHUTDOWN_EVENT: shutdown_event,
	notification.type_PARTIAL_DELIVERY_EVENT: pdapi_event,
	notification.type_ADAPTATION_INDICATION: adaptation_event,
}


def notification_factory(raw_notification):
	"""
	This function builds a notification based on a raw dictionary 
	received from the bottom-half (C language) of pysctp. The user
	should never need to call this directly.

	If the raw notification if of an unknown type, a "notification"
	superclass object is returned instead, so the user has a chance
	to test "type" and deal with that. A warning is sent to the 
	stderr. It can well happen if SCTP is extended and the user is 
	using an old version of PySCTP...

	If sctpsocket.unexpected_event_raises_exception (default=False) 
	is set to True, an unknown even will raise an exception (it is
	not raised by this function, but it seemed important to note it
	here too).

	"""
	try:
		num_type = raw_notification["type"]
	except:
		raise ValueError("Dictionary passed as parameter has no 'type' attribute")

	if num_type not in notification_table:
		# raw object since we do not know the type of notification
		o = notification(raw_notification)
		print("Warning: an unknown notification event (value %d) has arrived" % \
			  num_type, file=sys.stderr)
	else:
		o = notification_table[num_type](raw_notification)
	return o

########### EVENT SUBSCRIBING CLASS

class event_subscribe(object):
	"""
	This class implements subproperties for sctpsocket.events "property". 

	Variables and methods:

	autoflush: Boolean value that indicates if properties settings should be
		   immediately transmitted to kernel. Default = True.

	flush(): Flushes the properties to the kernel. If autoflush is True, it will
		 be called every time a property is set.

	clear(): Sets all properties to False, except data_io that is always set to
		 True. Guarantees that sctp_recv() will only return when an actual 
		 data message is received.

	When the class is insantiated, it gets the kernel default values. So, if the
	application just want to change one or two properties from the implementation
	default, it should not need to use clear().

	Properties: Every property sets whether a event class will or not be received 
	via recvmsg() or sctp_recv().

	data_io: refers to sndrcvinfo() (*)
	association: refers to assoc_change() event
	address: refers to paddr_change() event
	send_failure: refers to send_failed() event
	peer_error: refers to remote_error() event
	shutdown: refers to shutdon_event() 
	partial_delivery: refers to pdapi_event()
	adaptation_layer: refers to adaptation_event()

	(*) sndrcvinfo is ALWAYS returned by sctp_recv() along with message data. The
	    data_io property just controls whether sndrcvinfo() contains useful data.

	Convenience aliases for properties:

	dataio = data_io
	sndrcvinfo = data_io
	sendfailure = send_failure
	peererror = peer_error
	partialdelivery = partial_delivery
	adaptationlayer = adaptation_layer
	"""

	def flush(self):
		"""
		Flushes all event properties to the kernel.
		"""
		self.container._set_events(self.__dict__)
	
	def __set_property(self, key, value):
		self.__dict__[key] = value
		if self.autoflush:
			self.flush()

	def __get_property(self, key):
		return self.__dict__[key]

	def get_adaptation_layer(self):
		return self.__get_property("_adaptation_layer")

	def get_partial_delivery(self):
		return self.__get_property("_partial_delivery")

	def get_shutdown(self):
		return self.__get_property("_shutdown")

	def get_peer_error(self):
		return self.__get_property("_peer_error")

	def get_send_failure(self):
		return self.__get_property("_send_failure")

	def get_address(self):
		return self.__get_property("_address")

	def get_association(self):
		return self.__get_property("_association")

	def get_data_io(self):
		return self.__get_property("_data_io")

	def set_adaptation_layer(self, value):
		self.__set_property("_adaptation_layer", value)

	def set_partial_delivery(self, value):
		self.__set_property("_partial_delivery", value)

	def set_shutdown(self, value):
		self.__set_property("_shutdown", value)

	def set_peer_error(self, value):
		self.__set_property("_peer_error", value)

	def set_send_failure(self, value):
		self.__set_property("_send_failure", value)

	def set_address(self, value):
		self.__set_property("_address", value)

	def set_association(self, value):
		self.__set_property("_association", value)

	def set_data_io(self, value):
		self.__set_property("_data_io", value)

	def clear(self):
		"""
		Sets all event properties do False, except data_io what is set to True.
		Will NOT autoflush to kernel if autoflush variable is False.
		"""
		self._data_io = 1
		self._association = 0
		self._address = 0
		self._send_failure = 0
		self._peer_error = 0
		self._shutdown = 0
		self._partial_delivery = 0
		self._adaptation_layer = 0

		if self.autoflush:
			self.flush()

	def __init__(self, container):
		self.autoflush = False
		self.clear()
		self.autoflush = True

		self.container = container

		self.__dict__.update(self.container._get_events())

	# synonyms :)
	data_io = property(get_data_io, set_data_io)
	dataio = data_io
	sndrcvinfo = data_io

	association = property(get_association, set_association)

	address = property(get_address, set_address)

	send_failure = property(get_send_failure, set_send_failure)
	sendfailure = send_failure

	peer_error = property(get_peer_error, set_peer_error)
	peererror = peer_error

	shutdown = property(get_shutdown, set_shutdown)

	partial_delivery = property(get_partial_delivery, set_partial_delivery)
	partialdelivery = partial_delivery

	adaptation_layer = property(get_adaptation_layer, set_adaptation_layer)
	adaptationlayer = adaptation_layer


########## STRUCTURES EXCHANGED VIA set/getsockopt() 

class rtoinfo(object):
	"""
	Retransmission Timeout object class. This object can be read from a SCTP socket
	using the get_rtoinfo() method, and *written* to the socket using set_rtoinfo()
	socket method.

	Althrough the user could create a new rtoinfo() and fill it, it is better to get
	the current RTO info, change whatever is necessary and send it back.

	Relevant attributes:

	assoc_id: the association ID where this info came from, or where this information
		  is going to be applied to. Ignored/unreliable for TCP-style sockets 
		  because they hold only one association.

	initial: Initial RTO in milisseconds

	max: Maximum appliable RTO in milisseconds

	min: Minimum appliable RTO in milisseconds.
	"""
	def __init__(self):
		self.assoc_id = 0
		self.initial = 0
		self.max = 0
		self.min = 0

class assocparams(object):
	"""
	Association Parameters object class. This object can be read from a SCTP socket
	using the get_assocparams() socket method, and *written* to the socket using the
	set_assocparams() socket method.

	However, not all association properties are read-write. In the following list,
	read-write parameters are marked (rw), and read-only are marked (ro). The read-only
	behaviour is NOT enforced i.e. you can change an attribute in Python and it
	will not "stick" at socket level :)

	If a read-write attribute is passed as 0, it means "do not change it".

	FROM TSVWG document: "The maximum number of retransmissions before an address is considered
        unreachable is also tunable, but is address-specific, so it is covered in a separate option.
	If an application attempts to set the value of the association maximum retransmission 
	parameter to more than the sum of all maximum retransmission parameters, setsockopt()
	shall return an error."

	Althrough the user could create a new object and fill it, it is better to get
	the current parameters object, change whatever is necessary and send it back.

	assoc_id: the association ID where this info came from, or where this information
		  is going to be applied to. Ignored/unreliable for TCP-style sockets 
		  because they hold only one association.

	assocmaxrxt(rw): Maximum retransmission attempts to create an association

	number_peer_destinations(ro): Number of possible destinations a peer has

	peer_rwnd(ro): Peer's current receiving window, subtracted by in-flight data.

	local_rwnd(ro): Current local receiving window
 
	cookie_life(rw): Cookie life in miliseconds (cookies are used while creating an association)
	"""
	def __init__(self):
		self.assoc_id = 0
		self.assocmaxrxt = 0
		self.number_peer_destinations = 0
		self.peer_rwnd = 0
		self.local_rwnd = 0
		self.cookie_life = 0

class paddrparams(object):
	"""
	Peer Address Parameters object class. This object can be read from a SCTP socket
	using the get_paddrparms() method, and *written* to the socket using the
	set_paddrparams() socket method.

	When passing this object to set peer address parameters, attributes passed as
	zero mean "do not change this one". This makes easier for the user to create a
	new object, fill and sent it, but it is easier, and better, to get the current
	parameters object, change whatever is necessary and send it back.

	assoc_id: the association ID where this info came from, or where this information
		  is going to be applied to. Ignored/unreliable for TCP-style sockets 
		  because they hold only one association.

	sockaddr: the address this info came from, or where this information is going to
		  be applied to. It *is* important even for TCP-style sockets.

	hbinterval: heartbeat interval in milisseconds

	pathmaxrxt: maximum number of retransmissions, after that this peer address
		    is considered unreachable and another one will be tried.

	WARNING: THE FOLLOWING ATTRIBUTES WILL BE READ/WRITABLE ONLY IF YOUR
	         UNDERLYING SCTP IMPLEMENTATION IS AT LEAST DRAFT 10-LEVEL.

	pathmtu: Path MTU while using this peer address as path

	sackdelay: when delayed Selective ACK is enabled, this value specifies the
	           delay in milisseconds, for this peer address.

	flags: a bitmap with any subset of paddrparams.flags_* values active
	       test with Logical AND (& operand) instead of =

	       flags_HB_DISABLED: disable/disabled heartbeats for that peer
	       flags_HB_ENABLED: enable/enabled heartbeats

	       flags_SACKDELAY_*: the same song, for Selective ACK delay features

	       flags_PMTUD_*: the same song, but for the PMTU discovery feature.
	       
	       For every feature, there are two different flags for ENABLE and DISABLE
	       because this allows to pass a zeroed flag which means "do not toggle
	       any feature". Note that *simultaneous* use of *_ENABLED and *_DISABLED 
	       for the same feature will have undefined results! 
	       

	"""
	def __init__(self):
		self.assoc_id = 0
		self.sockaddr = ("", 0)
		self.hbinterval = 0
		self.pathmaxrxt = 0
		self.pathmtu = 0;
		self.sackdelay = 0;
		self.flags = 0;

	flags_HB_DISABLED = _sctp.getconstant("SPP_HB_DISABLED")
	flags_HB_ENABLED  = _sctp.getconstant("SPP_HB_ENABLED")
	flags_PMTUD_DISABLED = _sctp.getconstant("SPP_PMTUD_DISABLED")
	flags_PMTUD_ENABLED  = _sctp.getconstant("SPP_PMTUD_ENABLED")
	flags_SACKDELAY_DISABLED = _sctp.getconstant("SPP_SACKDELAY_DISABLED")
	flags_SACKDELAY_ENABLED  = _sctp.getconstant("SPP_SACKDELAY_ENABLED")

	# synthatical sugar
	flags_HB_DISABLE = flags_HB_DISABLED
	flags_HB_ENABLE  = flags_HB_ENABLED
	flags_PMTUD_DISABLE = flags_PMTUD_DISABLED
	flags_PMTUD_ENABLE  = flags_PMTUD_ENABLED
	flags_SACKDELAY_DISABLE = flags_SACKDELAY_DISABLED
	flags_SACKDELAY_ENABLE  = flags_SACKDELAY_ENABLED

class paddrinfo(object):
	"""
	Peer Address information object class. The user should never need to 
	instantiate this directly.  It is received via get_paddrinfo() method call.  It
	is read-only, you cannot set anything by passing this object.

	Relevant attributes:

	assoc_id: the association ID that this object refers to. May not have an useful value
		  if the object came from a TCP-style socket that holds only 1 association.

	sockaddr: address/port pair tuple that is the address being reported about

	state: either state_INACTIVE or state_ACTIVE

	cwnd: current congestion window size

	srtt: current estimate of round-trip time

	rto: current retransmission timeout 

	mtu: current MTU
	"""
	def __init__(self):
		self.assoc_id = 0	  # passed to getsockopt() (UDP)
		self.sockaddr = ("", 0)   # passed to getsockopt()
		self.state = 0 
		self.cwnd = 0
		self.srtt = 0
		self.rto = 0
		self.mtu = 0
        
	state_INACTIVE = _sctp.getconstant("SCTP_INACTIVE")
	state_ACTIVE = _sctp.getconstant("SCTP_ACTIVE")

class status(object):
	"""
	SCTP Association Status Report object class. The user should never need to 
	instantiate this directly.  It is received via get_status() socket method call. 
	It is read-only, you cannot set anything using this object.
	
	Relevant attributes:

	assoc_id: the Association ID the information refers to. Should be ignored if
	the object came from a TCP-style socket, which can hold only one association.

	state: The state of association. May be one of the status.state_* constants.

		WARNING: BOUND and LISTEN states are supported only on draft 10-level
			implementations.

	rwnd: Peer's current receiving window size.

	unackdata: unACK'ed data chuncks (not bytes)

	penddata: number of data chunks pending receipt

	primary: paddrinfo() object containing information about primary peer address.
		 See paddrinfo docstring for specific details.

	instrms: number of inbound streams. (The funnyish name was copied directly from
		 the C structure, we try to keep object attributes' names as near as possible
		 to the C API...)

	outstrms: number of outbound streams

	fragmentation_point: "The size at which SCTP fragmentation will occur." (tsvwg)
	"""
	def __init__(self):
		self.assoc_id = 0   # passed by caller via getsockopt() [UDP]
		self.state = 0
		self.rwnd = 0
		self.unackdata = 0
		self.penddata = 0
		self.instrms = 0
		self.outstrms = 0
		self.fragmentation_point = 0
		self.primary = paddrinfo()

	state_EMPTY = _sctp.getconstant("SCTP_EMPTY")
	state_CLOSED = _sctp.getconstant("SCTP_CLOSED")
	state_COOKIE_WAIT = _sctp.getconstant("SCTP_COOKIE_WAIT")
	state_COOKIE_ECHOED = _sctp.getconstant("SCTP_COOKIE_ECHOED")
	state_ESTABLISHED = _sctp.getconstant("SCTP_ESTABLISHED")
	state_SHUTDOWN_PENDING = _sctp.getconstant("SCTP_SHUTDOWN_PENDING")
	state_SHUTDOWN_SENT = _sctp.getconstant("SCTP_SHUTDOWN_SENT")
	state_SHUTDOWN_RECEIVED = _sctp.getconstant("SCTP_SHUTDOWN_RECEIVED")
	state_SHUTDOWN_ACK_SENT = _sctp.getconstant("SCTP_SHUTDOWN_ACK_SENT")
	state_BOUND = _sctp.getconstant("SCTP_BOUND")
	state_LISTEN = _sctp.getconstant("SCTP_LISTEN")

######### IMPLEMENTATION FEATURE LIST BITMAP

def features():
	"""
	Returns a bitmap of features possessed by the SCTP underlying
	implementation. Individual bits can be tested against
	HAVE_* constants.

	These flags may not be completely trustable, even in the sense that
	some feature may be available despite absence of flag. LK-SCTP 1.0.3 has 
	no HAVE_SCTP_PRSCTP macro, but Linux implements PR-SCTP extension
	since kernel 2.6.10...
	"""
	flags = HAVE_SCTP | HAVE_KERNEL_SCTP
	if _sctp.have_sctp_multibuf(): 
		flags |= HAVE_SCTP_MULTIBUF
	if _sctp.have_sctp_noconnect(): 
		flags |= HAVE_SCTP_NOCONNECT
	if _sctp.have_sctp_prsctp(): 
		flags |= HAVE_SCTP_PRSCTP
	if _sctp.have_sctp_addip(): 
		flags |= HAVE_SCTP_ADDIP
	if _sctp.have_sctp_setprimary(): 
		flags |= HAVE_SCTP_CANSET_PRIMARY
	if _sctp.have_sctp_sat_network(): 
		flags |= HAVE_SCTP_SAT_NETWORK_CAPABILITY
	return flags

#################### THE REAL THING :)

class sctpsocket(object):
	"""
	This is the base class for SCTP sockets. In general, the user will use sctpsocket_tcp()
	and sctpsocket_udp(), althrough it can use directly this class because all required
	functionality is, by design, implemented *here*. The subclasses are offered just for
	user convenience.

	This class does NOT inherit from python standard sockets. It CONTAINS a python socket
	and DELEGATES unknown method calls to that socket. So, we expect that sctpsocket 
	objects can be used in most places where a regular socket is expected. 

	Main methods:

	bindx: allows to bind to a set of network interfaces (standard bind() allows
	bind to exactly one or then to all).
	connectx: allows to connect by specifying a list of remote addresses.

	(For those that do not know SCTP: SCTP allows "multihomed" associations, i.e.
	connections over a set of peer addresses. If the primary address fails to
	respond (e.g. due to a network failure) it falls back to a secondary address.
	It is intended for automatic network redundancy.)
	
	getpaddrs: Gets the list of remote peer addresses for an association
	getladdrs: Gets the list of local addresses for an association
	sctp_send: Sends a SCTP message.  Allows to pass some SCTP-specific parameters.
		   If the specific parameters are not relevant, send() or sendto()
		   can also be used for SCTP.
	sctp_recv: Receives a SCTP messages. Returns SCTP-specific metadata along
		   with the data. If the metadata is not relevant for the 
		   application, recv()/recvfrom() and read() will also work.
	peeloff: Detaches ("peels off") an association from an UDP-style socket.
	accept: Overrides socket standard accept(), works the same way.
	set_peer_primary: Sets the peer primary address 
	set_primary: Set the local primary address

	In addition, socket methods like bind(), connect() close(), set/getsockopt()
	should work as expected. We tried to implement or override just the socket
	methods that have special behaviour in SCTP.

	Delegation methods (use with care):

	sock(): returns the contained Python standard socket.
	__getattr__(): this method does the delegation magic, forwarding all 
		       unknown attribute inquiries to the python socket.

	Properties:
	
	nodelay: If True, disables Nagle algorithm, which causes any message to be immediately
		 sent, potentially creating too much "tinygrams". Because of message-oriented
		 nature of SCTP, some implementations have it turned on by default, while TCP 
		 is mandated to have it off by default.

	adaptation: 32-bit value related to the "ppid" metadata field sent along each data
		  message. If this property is different than zero, the configured value
		  is sent via ADAPTION INDICATION event to the remote peer when a new 
		  association is opened. This is intended to be used by telephony-related
		  protocols.

	disable_fragments: if True, a message will never be fragmented in several datagrams.
			   It means that message must fit in the PMTU datagram. Default is
			   False for most implementations.

	mappedv4: If True, all IPv4 addresses will be received as IPv6-mapped IPv4 addresses
		  (::ffff:0:0/96). The default is True. Otherwise, the application can receive
		  either pure IPv6 or IPv4 addresses.

	maxseg: Maximum segment size i.e. the size of a message chunk inside a datagram,
	        in bytes. This value indirectly limits the size of the whole datagram, since
		datagram = maxseg + a fixed overhead.

	autoclose: For UDP-style sockets, sets/gets the auto-closing timeout for idle 
		   associations, in seconds. A value of 0 means that no automatic close
		   will be done. This property does not work for TCP-style sockets.

	ttl: Default timetolive value to use with sctp_send. Default set to 0.

	streamid: Default SCTP stream identifier value to use with sctp_send. Default set to 0.

	IMPORTANT NOTE: the maximum message size is limited both by the implementation 
	and by the transmission buffer (SO_SNDBUF). SCTP applications must configure 
	the transmission and receiving bufers accordingly to the biggest messages it
	excpects to deal with.

	"""
	def __init__(self, family, style, sk):
		"""
		Parameters:

		family: Internet address family (socket.AF_INET or socket.AF_INET6)

		style: TCP_STYLE or UDP_STYLE.

		sk: Underlying Python socket. If None is passed, it is created. When passing an
		    already-created socket, be sure it matches family and style!
		"""

		if not sk:
			sk = socket.socket(family, style, IPPROTO_SCTP)

		self._style = style
		self._sk = sk
		self._family = family
		self._ttl = 0
		self._streamid = 0

		self.unexpected_event_raises_exception = False
		self.initparams = initparams(self)
		self.events = event_subscribe(self)

		self.datalogging = False

	def bindx(self, sockaddrs, action=BINDX_ADD):
		"""
		Binds to a list of addresses. This method() allows to bind to any subset 
		of available interfaces, while standard bind() allows only one specific
		interface, or all of them using the INADDR_ANY pseudo-address. Also, it
		allows to *remove* the binding from one or more addresses.

		If you don't need the extended functionality of bindx(), standard bind()
		can be used and works as expected for SCTP.
		
		Parameters:

		sockaddr: List of (address, port) tuples.
		action: BINDX_ADD or BINDX_REMOVE. Default is BINDX_ADD.

		bindx() raises an exception if bindx() is not successful.
		"""
		_sctp.bindx(self._sk.fileno(), sockaddrs, action)

	def connectx(self, sockaddrs, assoc_id=None):
		"""
		Connects to a remote peer. It works like standard connect(), but accepts
		a list of address/port pairs, in order to support the SCTP multihoming

		Parameters:

		sockaddrs: List of (address, port) tuples.

		connectx() raises an exception if it is not successful. Warning: not all 
		SCTP implementations support connectx(). It will raise an RuntimeError()
		if not supported.
		"""
		if "connectx" in _sctp.__dict__:
			_sctp.connectx(self._sk.fileno(), sockaddrs, assoc_id)
		else:
			raise RuntimeError("Underlying SCTP implementation does not have connectx()")

	def getpaddrs(self, assoc_id = 0): # -> tuple of sockaddrs as strings
		"""
		Gets a list of remote address/pair tuples from an specific association.

		Parameters:

		assoc_id: Association ID of the association to be queried. If the socket is
			  TCP-style, this parameter is ignored.

		Returns: a list of address/port tuples.
		"""
		
		return _sctp.getpaddrs(self._sk.fileno(), assoc_id)

	def getladdrs(self, assoc_id = 0): # -> tuple of sockaddrs as strings
		"""
		Gets a list of local address/pair tuples from an specific association.

		Parameters:

		assoc_id: Association ID of the association to be queried. If the socket is
			  TCP-style, this parameter is ignored. If zero is passed for an
			  UDP-style socket, it refers to the socket default local address.

		Returns: a list of address/port tuples. 
		
		Note that if you do not bind() explicitely (e.g. in client-side programs), 
		this method will return only one wildcard address, which is useful only
		to determinate the ephemeral port that the client is using. 
		"""

		return _sctp.getladdrs(self._sk.fileno(), assoc_id)

	def sctp_send(self, msg, to=("",0), ppid=None, flags=0, stream=None, timetolive=None, context=0,
	                    record_file_prefix="RECORD_sctp_traffic", datalogging = False):
		"""
		Sends a SCTP message. While send()/sendto() can also be used, this method also
		accepts some SCTP-exclusive metadata. Parameters:

		msg: string containing the message to be sent.

		to: an address/port tuple identifying the destination, or the assoc_id for the
		    association. It can (and generally must) be omitted for TCP-style sockets.

		    WARNING: identifying destination by Association ID not implemented yet!

		ppid: adaptation layer value, a 32-bit metadata that is sent along the message.
		      Default to 0.

		flags: a bitmap of MSG_* flags. For example, MSG_UNORDERED indicates that 
		       message can be delivered out-of-order, and MSG_EOF + empty message 
		       shuts down the association. Defaults to no flags set.

		       It does NOT include flags like MSG_DONTROUTE or other low-level flags
		       that are supported by sendto().

		stream: stream number where the message will sent by.
				If not set use default value.

		timetolive: time to live of the message in milisseconds. Zero means infinite
		            TTL. If TTL expires, the message is discarded. Discarding policy
			    changes whether implementation implements the PR-SCTP extension or not.
			    If not set use default value.

		context: an opaque 32-bit integer that will be returned in some notification events,
		         if the event is directly related to this message transmission. So the
			 application can know exactly which message triggered which event.
			 Defaults to 0.
		
		The method returns the number of bytes sent. Ideally it is going to be exacly the
		size of the message. Transmission errors will trigger an exception.
		

		WARNING: the maximum message size that can be sent via SCTP is limited
		both by the implementation and by the transmission buffer (SO_SNDBUF).
		The application must configure this buffer accordingly.
		"""

		if ppid is None:
			ppid = self.adaptation

		if timetolive is None:
			timetolive = self._ttl

		if stream is None:
			stream = self._streamid

		if datalogging == True or self.datalogging == True:
			now = datetime.datetime.now()
			recordfilename = record_file_prefix + "-" + now.strftime("%Y%m%d%H%M%S") + "-c2s."
			i = 1
			while os.path.exists(recordfilename+"%d"%i):
				i = i + 1
			recordlog = open(recordfilename+"%d"%i, 'w')
			recordlog.write(msg)
			recordlog.close()
		return _sctp.sctp_send_msg(self._sk.fileno(), msg, to, ntohl(ppid), flags, stream, timetolive, context)

	def sctp_recv(self, maxlen):
		"""
		Receives an SCTP message and/or a SCTP notification event. The notifications
		that can be received are regulated by "events" property and its subproperties.
		See event_subscribe() class for details.

		It is important to know that sctp_recv() can return either on data messages or
		on subscribed events. If the application just wants data, it must unsubscribe
		the other events.

		Parameters;

		maxlen: the maximum message size that can be received. If bigger messages are
		received, they will be received in fragments (non-atomically). The application 
		must choose this carefully if it wants to keep the atomicity of messages!

		Returns: (fromaddr, flags, msg, notif)

		fromaddr: address/port pair. For some applications, association ID will be more
		          useful to identify the related association. Fortunately, assoc_id is
			  a attribute of most notifications received via "notif" (see below)

		flags: a bitmap of lower-level recvmsg() flags (FLAG_* flags).  
		
			FLAG_NOTIFICATION indicates that an event notification was 
			returned, instead of a data message.

		 	FLAG_EOR indicates that this is the final fragment of a data message.
			Ideally, all messages will come with this flag set.

		       WARNING: message data-related flags like MSG_UNORDERED are returned
		       inside sndrcvinfo() notifications, and NOT here!

		msg: the actual data message. Since SCTP does not allow empty messages,
		     an empty "msg" always means something special, either:

		    	a) that "notif" contains an event notificatoin, if "flags" has 
			   FLAG_NOTIFICATION set;

			b) that association is closing (for TCP-style sockets only)

		notif: notification event object. If "msg" is a data message, it will contain
		       a sndrcvinfo() object that contains metadata about the message. If 
		       "flags" has FLAG_NOTIFICATION set, it will contain some notification()
		       subclass.

		       sndrcvinfo() is ALWAYS returned when a data message is received, but
		       it will only contain useful data IF events.data_io is subscribed True.

		WARNING: the maximum message size that can be received via SCTP is limited:

		* by the underlying implementation. Check your operating system.

		* by the "maxlen" parameter passed. If message is bigger, it will be received
		  in several fragments. The last fragment will have FLAG_EOR flag set.

		* by the socket's reception buffer (SO_SNDRCV). The application must configure 
		  this buffer accordingly, otherwise the message will be truncacted.
		"""
		(fromaddr, flags, msg, _notif) = _sctp.sctp_recv_msg(self._sk.fileno(), maxlen)

		if (flags & FLAG_NOTIFICATION):
			notif = notification_factory(_notif)
			if (notif.__class__ == notification):
				# Raw notification class, means we do not know the exact type
				# of this notification
				if self.unexpected_event_raises_exception:
					raise IOError("An unknown event notification has arrived")
		else:
			notif = sndrcvinfo(_notif)
		
		return (fromaddr, flags, msg, notif)

	def peeloff(self, assoc_id): 
		"""
		Detaches ("peels off") an association from an UDP-style socket.
		Will throw an IOError exception if it is not successful.
		
		Parameters:

		assoc_id: Association ID to be peeled off

		Returns: a sctpsocket_tcp() object. 
		
		"""
		fd = _sctp.peeloff(self._sk.fileno(), assoc_id)
		if fd < 0:
			raise IOError("Assoc ID does not correspond to any open association")

		sk = socket.fromfd(fd, self._family, SOCK_STREAM, IPPROTO_SCTP)
		return sctpsocket_tcp(self._family, sk)
	
	def accept(self): 
		"""
		Frontend for socket.accept() method. Does exactly the same thing,
		except that returns an sctpsocket_tcp object instead of a standard
		Python socket. May throw the same exceptions as socket.accept()

		Returns: a sctpsocket_tcp() object
		"""
		sk, fromaddr = self._sk.accept()
		if sk:
			return (sctpsocket_tcp(self._family, sk), fromaddr)
		else:
			raise IOError("sctpsocket.accept() failed for unknown reason")
	
	def set_peer_primary(self, assoc_id, addr):
		"""
		Requests the remote peer to use our [addr] as the primary address. Parameters:

		assoc_id: the association to be affected. Pass zero for TCP-style sockets.

		addr: address/port pair tuple. Be sure to pass a correct tuple, with the
		      right local port number (that can be discovered via getladdrs()).

		Raises an exception if not successful. Some implementations may not support it.
		It seems to work only at server-side.
		"""

		if self._style == TCP_STYLE:
			if assoc_id != 0:
				raise ValueError("assoc_id is ignored for TCP-style sockets, pass 0")
		else:
			if assoc_id == 0:
				# raise ValueError("assoc_id is needed for UDP-style sockets")
				# FIXME
				pass
		
		_sctp.set_peer_primary(self._sk.fileno(), assoc_id, addr)

	def set_primary(self, assoc_id, addr):
		"""
		Requests the local SCTP stack to use the peer's primary address "addr".
		Parameters:

		assoc_id: the association to be affected. Pass zero for TCP-style sockets.
		addr: address/port pair tuple

		Raises an exception if not successful. Some implementations may not support it.
		"""

		if self._style == TCP_STYLE:
			if assoc_id != 0:
				raise ValueError("assoc_id is ignored for TCP-style sockets, pass 0")
		else:
			if assoc_id == 0:
				raise ValueError("assoc_id is needed for UDP-style sockets")
		
		_sctp.set_primary(self._sk.fileno(), assoc_id, addr)

	# Properties

	def _get_initparams(self):
		"""
		Private function, used only by initparams() class. Returns a dictionary
		of default association init parameters for this socket (initparams property and
		initparams() class that contain the subproperties).
		"""
		return _sctp.get_initparams(self._sk.fileno())

	def _set_initparams(self, d):
		"""
		Private function, used only by initparams() class. Sets the default
		association parameters for this socket (initparams property and
		initparams() class that contain the subproperties).
		"""
		_sctp.set_initparams(self._sk.fileno(), d)

	def get_nodelay(self):
		"""
		Gets the status of NODELAY for the socket from the kernel.

		See class documentation for more details. (nodelay property)
		"""
		return _sctp.get_nodelay(self._sk.fileno())

	def set_nodelay(self, rvalue):
		"""
		Sets the status of NODELAY for the socket.

		See class documentation for more details. (nodelay property)
		"""
		_sctp.set_nodelay(self._sk.fileno(), rvalue)

	def get_adaptation(self):
		"""
		Gets the adaptation layer indication from kernel.
		
		See class documentation for more details. (adaptation property)
		"""
		return _sctp.get_adaptation(self._sk.fileno())

	def set_adaptation(self, rvalue):
		"""
		Sets the adaptation layer indication for this socket.

		See class documentation for more details. (adaptation property)
		"""
		_sctp.set_adaptation(self._sk.fileno(), rvalue)

	def get_sndbuf(self):
		"""
		Gets the send buffer size from the kernel for this socket.
		"""
		return _sctp.get_sndbuf(self._sk.fileno())

	def set_sndbuf(self, rvalue):
		"""
		Sets the send buffer size in the kernel for this socket.
		"""
		# Linux doubles the size, hence we divise it by 2
		_sctp.set_sndbuf(self._sk.fileno(), rvalue//2)

	def get_rcvbuf(self):
		"""
		Gets the receive buffer size from the kernel for this socket.
		"""
		return _sctp.get_rcvbuf(self._sk.fileno())

	def set_rcvbuf(self, rvalue):
		"""
		Sets the receive buffer size in the kernel for this socket.
		"""
		# Linux doubles the size, hence we divise it by 2
		_sctp.set_rcvbuf(self._sk.fileno(), rvalue//2)

	def get_disable_fragments(self):
		"""
		Queries kernel and returns True if message fragmenting is disable for
		this socket. 

		See class documentation for more details. (disable_fragments property)
		"""
		return _sctp.get_disable_fragments(self._sk.fileno())

	def set_disable_fragments(self, rvalue):
		"""
		Sets whether message fragmentation should be disable for this socket.
		
		See class documentation for more details. (disable_fragments property)
		"""
		_sctp.set_disable_fragments(self._sk.fileno(), rvalue)

	def _get_events(self):
		"""
		Private function, used only by event_subscribe() class. Returns a dictionary
		with the state of event subscriptions for this socket.

		See class documentation for more details. (events property and event_subscribe class)
		"""
		return _sctp.get_events(self._sk.fileno())

	def _set_events(self, d):
		"""
		Private function, used only by event_subscribe() class. Sets the event 
		subscriptions for this socket.

		See class documentation for more details. (events property and event_subscribe class)
		"""
		_sctp.set_events(self._sk.fileno(), d)

	def get_mappedv4(self):
		"""
		Returns True if IPv4 address are be returned as IPv4-mapped IPv6 addresses
		(::FFFF:0:0/96 range).
		"""
		return _sctp.get_mappedv4(self._sk.fileno())

	def set_mappedv4(self, rvalue):
		"""
		Sets whether IPv4 address should be returned as IPv4-mapped IPv6 addresses
		(::FFFF:0:0/96 range).
		"""
		_sctp.set_mappedv4(self._sk.fileno(), rvalue)

	def get_maxseg(self):
		"""
		Get the maximum segment size. i.e. the maximum size of a message chunk.
		"""
		return _sctp.get_maxseg(self._sk.fileno())

	def set_maxseg(self, rvalue):
		"""
		Set the maximum segment size i.e. the maximum size of a message chunk.
		It indirectly limits the maximum SCTP datagram size.
		"""
		_sctp.set_maxseg(self._sk.fileno(), rvalue)

	def get_autoclose(self):
		"""
		Gets the timeout value (in seconds) for idle associations, after
		which they will be closed. Zero means infinite timeout.
		"""
		return _sctp.get_autoclose(self._sk.fileno())

	def set_autoclose(self, rvalue):
		"""
		Sets the timeout of idle associations, in seconds. After that,
		associations are closed. A value of zero means they are never
		closed automatically.
		"""
		_sctp.set_autoclose(self._sk.fileno(), rvalue)

	def get_status(self, assoc_id = 0):
		"""
		Returns a status structure for an SCTP association. For more information
		about the returned data, see status() class docstring.
		
		Parameters:

		assoc_id: the association ID of the association. Must be zero or not passed at all
			  for TCP-style sockets.

		"""
		if self._style == TCP_STYLE:
			if assoc_id != 0:
				raise ValueError("assoc_id is ignored for TCP-style sockets, pass 0")
		else:
			if assoc_id == 0:
				raise ValueError("assoc_id is needed for UDP-style sockets")
		
		s = status()
		s.assoc_id = assoc_id
		_sctp.get_status(self._sk.fileno(), s.__dict__, s.primary.__dict__)

		return s

	def get_paddrinfo(self, assoc_id, sockaddr):
		"""
		Returns a paddrinfo() object relative to an association/peer address pair. 
		For more information about the returned data, see paddrinfo() class docstring.
		
		Parameters:

		assoc_id: the association ID of the association. Must be zero for TCP-style sockets.

		sockaddr: the address/port tuple. Due to the nature of the information, it can not
			  be a wildcard (there is no "association-wide" information here); a concrete
			  peer address must be passed.

		This method distinguishes from get_paddrparams() because information returned here
		is read-only -- there is NOT a set_paddrinfo() method. 

		A paddrinfo() object related to the primary association address is indirectly 
		returned by get_status(), in the status.primary attribute.
		"""
		if self._style == TCP_STYLE:
			if assoc_id != 0:
				raise ValueError("assoc_id is ignored for TCP-style sockets, pass 0")
		else:
			if assoc_id == 0:
				raise ValueError("assoc_id is needed for UDP-style sockets")
		
		s = paddrinfo()
		s.assoc_id = assoc_id
		s.sockaddr = sockaddr
		_sctp.get_paddrinfo(self._sk.fileno(), s.__dict__)

		return s

	def get_assocparams(self, assoc_id = 0):
		"""
		Returns a assocparams() object relative to a SCTP association. For more information
		about the returned data, see assocparams() class docstring.
		
		Parameters:

		assoc_id: the association ID of the association. Must be zero or not passed at all
			  for TCP-style sockets. If you pass a zero for UDP-style sockets, it will
			  return the default parameters of the whole endpoint.
		"""
		
		s = assocparams()
		s.assoc_id = assoc_id
		_sctp.get_assocparams(self._sk.fileno(), s.__dict__)

		return s

	def set_assocparams(self, o):
		"""
		Sets parameters for a SCTP association. Parameters:

		o: assocparams() object containing the assoc_id of the association be
		   affected, plus the association parameters. If assoc_id is zero and 
		   socket is UDP style, it will set the default parameters for future
		   associations.

		Warning: it seems not to work in TCP-style sockets, in client side.
			 (we do not know the reason.)

		It is advisable not to create the assocparams() object from scratch, but 
		rather get it with assocparms(), change whatever necessary and send it back.
		It guarantees that it has sensible defaults and a correct identification of
		the association to be affected.
		"""
		_sctp.set_assocparams(self._sk.fileno(), o.__dict__)

	def get_paddrparams(self, assoc_id, sockaddr):
		"""
		Returns a paddrparams() object relative to an association/peer address pair. 
		For more information about the returned data, see paddrparams() class docstring.
		
		Parameters:

		assoc_id: the association ID of the association. Must be zero for TCP-style sockets.

		sockaddr: the address/port tuple. Due to the nature of the information, it can not
			  be a wildcard (there is no "association-wide" information here); a concrete
			  peer address must be passed.

			  Note: For TCP-style, client-side sockets, only the wildcard address
			        ("",0) seems to work. We don't know exactly why :)

		This method distinguishes from get_paddrinfo() because information returned here
		can be changed at socket level, while *paddrinfo() is read-only.
		"""
		if self._style == TCP_STYLE:
			if assoc_id != 0:
				raise ValueError("assoc_id is ignored for TCP-style sockets, pass 0")
		else:
			if assoc_id == 0:
				raise ValueError("assoc_id is needed for UDP-style sockets")
		
		s = paddrparams()
		s.assoc_id = assoc_id
		s.sockaddr = sockaddr
		_sctp.get_paddrparams(self._sk.fileno(), s.__dict__)

		return s

	def set_paddrparams(self, o):
		"""
		Sets peer address parameters for a SCTP association. Parameters:

		o: paddrparams() object containing the assoc_id/address of the peer to be
		   affected, plus the address parameters.

		It is advisable not to create the paddrparams() object from scratch, but 
		rather get it with get_paddrparms(), change whatever necessary and send it back.
		It guarantees that it has sensible defaults and a correct identification of
		the association/address to be affected.
		"""
		_sctp.set_paddrparams(self._sk.fileno(), o.__dict__)

	def get_rtoinfo(self, assoc_id = 0):
		"""
		Returns a RTO information structure for a SCTP association. For more information
		about the returned data, see rtoinfo() class docstring.
		
		Parameters:

		assoc_id: the association ID of the association. Must be zero or not passed at all
			  for TCP-style sockets. If zero is passed for UDP-style sockets, the
			  information refers to the socket defaults, and not any particular
			  opened association.
		"""
		
		s = rtoinfo()
		s.assoc_id = assoc_id
		_sctp.get_rtoinfo(self._sk.fileno(), s.__dict__)

		return s

	def set_rtoinfo(self, o):
		"""
		Sets RTO parameters for a SCTP association. Parameters:

		o: rtoinfo() object containing the assoc_id of the association to be
		   affected, plus the RTO parameters. If an assoc_id of zero is passed
		   for UDP-style socket, the information will affect the socket defaults,
		   not any particular association.

		It is advisable not to create rtoinfo() from scratch, but rather get it
		from get_rtoinfo(), change whatever necessary and send it back.
		"""
		_sctp.set_rtoinfo(self._sk.fileno(), o.__dict__)

	def get_ttl(self):
		"""
		Read default time to live value, 0 mean infinite
		"""
		return self._ttl

	def set_ttl(self, newVal):
		"""
		Write default time to live
		"""
		if not isinstance(newVal, int) or newVal < 0 or newVal > 255:
			raise ValueError('TTL shall be >= 0 and <= 255')

		self._ttl = newVal

	def get_streamid(self):
		"""
		Read default stream identifier
 		"""
		return self._streamid

	def set_streamid(self, newVal):
		"""
		Write default stream identifier
		"""
		if not isinstance(newVal, int) or newVal < 0 or newVal > 65535:
			raise ValueError('streamid shall be a valid unsigned 16bits integer')

		self._streamid = newVal

	# delegation

	def sock(self):
		"""
		Returns the underlying Python socket of an SCTP socket object. Use with care.
		"""
		return self._sk
	
	def __getattr__(self, name):
		"""
		Delegation trick that routes every unknown attribute to the underlying
		Python socket (self._sk). This will allow a sctpsocket() to be used in most
		places where a standard socket or file is expected.
		"""
		return getattr(self._sk, name)

	# properties

	nodelay = property(get_nodelay, set_nodelay)
	adaptation = property(get_adaptation, set_adaptation)
	disable_fragments = property(get_disable_fragments, set_disable_fragments)
	mappedv4 = property(get_mappedv4, set_mappedv4)
	maxseg = property(get_maxseg, set_maxseg)
	autoclose = property(get_autoclose, set_autoclose)
	ttl = property(get_ttl, set_ttl)
	streamid = property(get_streamid, set_streamid)

class sctpsocket_tcp(sctpsocket):
	"""
	This class represents TCP-style SCTP sockets. Its objects can be used
	in most places where a standard Python socket is accepted.

	TCP-style sockets hold at most one association per socket, much like
	TCP sockets, hence the name.

	This class overrides peeloff() and makes it raise an exception because
	it is not valid for TCP-style sockets.The get/set methods for autoclose
	property are also disabled that way.

	getpaddrs() and getladdrs() methods are overriden so they can be called
	without the assoc_id parameter (since the association is implicit for
	this type of socket). Still, if a parameter is passed, it is simply 
	ignored, to avoid creating unnecessary interface differences between
	TCP and UDP-style sockets.

	set_primary() and set_peer_primary() are NOT overriden in order
	to omit the assoc_id, so be sure to pass a (zero) assoc_id.

	All functionalities are inherited from the sctpsocket() father class. 
	This class is just a front-end. For more documentation, take a look 
	at sctpsocket() class, as well as documentation of the methods.
	"""
	def __init__(self, family, sk=None):
		"""
		Creates a TCP-style SCTP socket. Parameters:

		family: socket.AF_INET or socket.AF_INET6

		sk: a Python socket, if it already exists. If not passed, a new
		    socket is automatically created. This is useful if a SCTP socket
		    is received as standard input/output and the application just
		    wants to "repackage" it as a sctpsocket_tcp()
		"""
		sctpsocket.__init__(self, family, TCP_STYLE, sk)

	def peeloff(self, *params):
		"""
		Stub method to block unappropriate calling.
		"""
		raise IOError("TCP-style sockets have no peeloff operation")

	def get_autoclose(self):
		"""
		Stub method to block unappropriate calling.
		"""
		raise IOError("TCP-style sockets do not support AUTOCLOSE")

	def set_autoclose(self, rvalue):
		"""
		Stub method to block unappropriate calling.
		"""
		raise IOError("TCP-style sockets do not support AUTOCLOSE")

class sctpsocket_udp(sctpsocket):
	"""
	This class represents UDP-style SCTP sockets. Its objects can be used
	in most places where a standard Python socket is accepted.

	An UDP-style SCTP socket can hold several associations via a single
	socket. In addition, associations will be opened automatically when a
	message is sent (via standard sendto() or SCTP-only sctp_send()). It
	resembles sockets communication via UDP, hence the name.

	It is opportune to remember that listen() must be called for UDP-style
	sockets too, because SCTP is, after all, a connection-oriented protocol.

	The associations are also closed automatically after a idle time. The
	timeout is acessible/configurable via sctpsocket.autoclose property.
	The value is in seconds, 120 is a common default. Setting it to zero
	means that associations are never (automatically) closed. 

	Associations can be opened and/or closed manually by passing MSG_EOF flag
	along an empty message (empty messages are not actually allowed by SCTP).
	
	All functionalities are inherited from the sctpsocket() father class. 
	This class is just a front-end. For more documentation, take a look 
	at sctpsocket() class, as well as documentation of the methods.
	"""
	def __init__(self, family, sk=None):
		"""
		Creates a sctpsocket_udp() object. Parameters: 
		
		family: socket.AF_INET or socket.AF_INET6.

		sk (optional): Normally. sctpsocket() creates its own socket.
		if "sk" is passed, it will use the passed socket instead. Make
		sure it matches address family, protocol (SCTP) and style. In
		general, users should never need to pass this parameter.
		"""
		sctpsocket.__init__(self, family, UDP_STYLE, sk)

	def accept(self, *params):
		"""
		Stub method to block unappropriate calling.
		"""
		raise IOError("UDP-style sockets have no accept() operation")

