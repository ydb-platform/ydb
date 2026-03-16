from rstr.xeger import Xeger
from rstr.rstr_base import SameCharacterError

Rstr = Xeger
_default_instance = Rstr()

rstr = _default_instance.rstr
xeger = _default_instance.xeger


# This allows convenience methods from rstr to be accessed at the package
# level, without requiring the user to instantiate an Rstr() object.
printable = _default_instance.printable
letters = _default_instance.letters
uppercase = _default_instance.uppercase
lowercase = _default_instance.lowercase
digits = _default_instance.digits
punctuation = _default_instance.punctuation
nondigits = _default_instance.nondigits
nonletters = _default_instance.nonletters
whitespace = _default_instance.whitespace
nonwhitespace = _default_instance.nonwhitespace
normal = _default_instance.normal
word = _default_instance.word
nonword = _default_instance.nonword
unambiguous = _default_instance.unambiguous
postalsafe = _default_instance.postalsafe
urlsafe = _default_instance.urlsafe
domainsafe = _default_instance.domainsafe
