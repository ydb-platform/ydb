#===============================================================================
# Copyright (c) 2015, Max Zwiessele
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# 
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
# 
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
# 
# * Neither the name of paramz.core.nameable nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#===============================================================================
from .gradcheckable import Gradcheckable
import re

def adjust_name_for_printing(name):
    """
    Make sure a name can be printed, alongside used as a variable name.
    """
    if name is not None:
        name2 = name
        name = name.replace(" ", "_").replace(".", "_").replace("-", "_m_")
        name = name.replace("+", "_p_").replace("!", "_I_")
        name = name.replace("**", "_xx_").replace("*", "_x_")
        name = name.replace("/", "_l_").replace("@", '_at_')
        name = name.replace("(", "_of_").replace(")", "")
        if re.match(r'^[a-zA-Z_][a-zA-Z0-9-_]*$', name) is None:
            raise NameError("name {} converted to {} cannot be further converted to valid python variable name!".format(name2, name))
        return name
    return ''


class Nameable(Gradcheckable):
    """
    Make an object nameable inside the hierarchy.
    """
    def __init__(self, name, *a, **kw):
        self._name = name or self.__class__.__name__
        super(Nameable, self).__init__(*a, **kw)

    @property
    def name(self):
        """
        The name of this object
        """
        return self._name
    @name.setter
    def name(self, name):
        """
        Set the name of this object.
        Tell the parent if the name has changed.
        """
        from_name = self.name
        assert isinstance(name, str)
        self._name = name
        if self.has_parent():
            self._parent_._name_changed(self, from_name)
            
    def hierarchy_name(self, adjust_for_printing=True):
        """
        return the name for this object with the parents names attached by dots.

        :param bool adjust_for_printing: whether to call :func:`~adjust_for_printing()`
                                         on the names, recursively
                                         
        """
        if adjust_for_printing: adjust = lambda x: adjust_name_for_printing(x)
        else: adjust = lambda x: x
        if self.has_parent():
            return self._parent_.hierarchy_name() + "." + adjust(self.name)
        return adjust(self.name)
