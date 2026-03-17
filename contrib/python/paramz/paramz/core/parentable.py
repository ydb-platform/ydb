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
# * Neither the name of paramz.core.parentable nor the names of its
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

class Parentable(object):
    """
    Enable an Object to have a parent.

    Additionally this adds the parent_index, which is the index for the parent
    to look for in its parameter list.
    """
    _parent_ = None
    _parent_index_ = None
    def __init__(self, *args, **kwargs):
        super(Parentable, self).__init__()

    def has_parent(self):
        """
        Return whether this parentable object currently has a parent.
        """
        return self._parent_ is not None

    def _parent_changed(self):
        """
        Gets called, when the parent changed, so we can adjust our
        inner attributes according to the new parent.
        """
        raise NotImplementedError("shouldnt happen, Parentable objects need to be able to change their parent")

    def _disconnect_parent(self, *args, **kw):
        """
        Disconnect this object from its parent
        """
        raise NotImplementedError("Abstract superclass")

    @property
    def _highest_parent_(self):
        """
        Gets the highest parent by traversing up to the root node of the hierarchy.
        """
        if self._parent_ is None:
            return self
        return self._parent_._highest_parent_

    def _notify_parent_change(self):
        """
        Dont do anything if in leaf node
        """
        pass
