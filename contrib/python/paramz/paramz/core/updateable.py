#===============================================================================
# Copyright (c) 2014-2015, Max Zwiessele
#
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
# * Neither the name of paramax nor the names of its
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
from .observable import Observable


class Updateable(Observable):
    """
    A model can be updated or not.
    Make sure updates can be switched on and off.
    """
    def __init__(self, *args, **kwargs):
        super(Updateable, self).__init__(*args, **kwargs)

    def update_model(self, updates=None):
        """
        Get or set, whether automatic updates are performed. When updates are
        off, the model might be in a non-working state. To make the model work
        turn updates on again.

        :param bool|None updates:

            bool: whether to do updates
            None: get the current update state
        """
        if updates is None:
            return self._update_on
        assert isinstance(updates, bool), "updates are either on (True) or off (False)"
        p = getattr(self, '_highest_parent_', None)
        def turn_updates(s):
            s._update_on = updates
        p.traverse(turn_updates)
        self.trigger_update()

    def toggle_update(self):
        print("deprecated: toggle_update was renamed to update_toggle for easier access")
        self.update_toggle()
    def update_toggle(self):
        self.update_model(not self.update_model())

    def trigger_update(self, trigger_parent=True):
        """
        Update the model from the current state.
        Make sure that updates are on, otherwise this
        method will do nothing

        :param bool trigger_parent: Whether to trigger the parent, after self has updated
        """
        if not self.update_model() or (hasattr(self, "_in_init_") and self._in_init_):
            #print "Warning: updates are off, updating the model will do nothing"
            return
        self._trigger_params_changed(trigger_parent)
