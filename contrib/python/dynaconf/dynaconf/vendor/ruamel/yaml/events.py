_A=None
if False:from typing import Any,Dict,Optional,List
def CommentCheck():0
class Event:
	__slots__='start_mark','end_mark','comment'
	def __init__(A,start_mark=_A,end_mark=_A,comment=CommentCheck):
		B=comment;A.start_mark=start_mark;A.end_mark=end_mark
		if B is CommentCheck:B=_A
		A.comment=B
	def __repr__(A):
		C=[B for B in['anchor','tag','implicit','value','flow_style','style']if hasattr(A,B)];B=', '.join(['%s=%r'%(B,getattr(A,B))for B in C])
		if A.comment not in[_A,CommentCheck]:B+=', comment={!r}'.format(A.comment)
		return'%s(%s)'%(A.__class__.__name__,B)
class NodeEvent(Event):
	__slots__='anchor',
	def __init__(A,anchor,start_mark=_A,end_mark=_A,comment=_A):Event.__init__(A,start_mark,end_mark,comment);A.anchor=anchor
class CollectionStartEvent(NodeEvent):
	__slots__='tag','implicit','flow_style','nr_items'
	def __init__(A,anchor,tag,implicit,start_mark=_A,end_mark=_A,flow_style=_A,comment=_A,nr_items=_A):NodeEvent.__init__(A,anchor,start_mark,end_mark,comment);A.tag=tag;A.implicit=implicit;A.flow_style=flow_style;A.nr_items=nr_items
class CollectionEndEvent(Event):__slots__=()
class StreamStartEvent(Event):
	__slots__='encoding',
	def __init__(A,start_mark=_A,end_mark=_A,encoding=_A,comment=_A):Event.__init__(A,start_mark,end_mark,comment);A.encoding=encoding
class StreamEndEvent(Event):__slots__=()
class DocumentStartEvent(Event):
	__slots__='explicit','version','tags'
	def __init__(A,start_mark=_A,end_mark=_A,explicit=_A,version=_A,tags=_A,comment=_A):Event.__init__(A,start_mark,end_mark,comment);A.explicit=explicit;A.version=version;A.tags=tags
class DocumentEndEvent(Event):
	__slots__='explicit',
	def __init__(A,start_mark=_A,end_mark=_A,explicit=_A,comment=_A):Event.__init__(A,start_mark,end_mark,comment);A.explicit=explicit
class AliasEvent(NodeEvent):__slots__=()
class ScalarEvent(NodeEvent):
	__slots__='tag','implicit','value','style'
	def __init__(A,anchor,tag,implicit,value,start_mark=_A,end_mark=_A,style=_A,comment=_A):NodeEvent.__init__(A,anchor,start_mark,end_mark,comment);A.tag=tag;A.implicit=implicit;A.value=value;A.style=style
class SequenceStartEvent(CollectionStartEvent):__slots__=()
class SequenceEndEvent(CollectionEndEvent):__slots__=()
class MappingStartEvent(CollectionStartEvent):__slots__=()
class MappingEndEvent(CollectionEndEvent):__slots__=()