# ===============================================================================
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
# ===============================================================================

from __future__ import print_function
import numpy as np
import sys
import time


def exponents(fnow, current_grad):
    exps = [np.abs(float(fnow)),
            1 if current_grad is np.nan else current_grad]
    return np.sign(exps) * np.log10(exps).astype(int)


class VerboseOptimization(object):
    def __init__(self, model, opt, maxiters, verbose=False, current_iteration=0, ipython_notebook=True, clear_after_finish=False):
        self.verbose = verbose
        if self.verbose:
            self.model = model
            self.iteration = current_iteration
            self.p_iter = self.iteration
            self.maxiters = maxiters
            self.len_maxiters = len(str(int(maxiters)))
            self.opt_name = opt.opt_name
            self.opt = opt
            self.model.add_observer(self, self.print_status)
            self.status = 'running'
            self.clear = clear_after_finish

            self.update()

            try:  # pragma: no cover
                from IPython.display import display
                from ipywidgets import IntProgress, HTML, Box, VBox, HBox
                self.text = HTML(width='100%')
                self.progress = IntProgress(min=0, max=maxiters)
                #self.progresstext = Text(width='100%', disabled=True, value='0/{}'.format(maxiters))
                self.model_show = HTML()
                self.ipython_notebook = ipython_notebook
                get_ipython
            except:
                # Not in Ipython notebook
                self.ipython_notebook = False

            if self.ipython_notebook:  # pragma: no cover
                left_col = VBox(
                    children=[self.progress, self.text], padding=2, width='40%')
                right_col = Box(
                    children=[self.model_show], padding=2, width='60%')
                self.hor_align = HBox(
                    children=[left_col, right_col], width='100%', orientation='horizontal')

                display(self.hor_align)

                try:
                    self.text.set_css('width', '100%')
                    left_col.set_css({
                        'padding': '2px',
                        'width': "100%",
                    })

                    right_col.set_css({
                        'padding': '2px',
                    })

                    self.hor_align.set_css({
                        'width': "100%",
                    })

                    self.hor_align.remove_class('vbox')
                    self.hor_align.add_class('hbox')

                    left_col.add_class("box-flex1")
                    right_col.add_class('box-flex0')

                except:
                    pass

                # self.text.add_class('box-flex2')
                # self.progress.add_class('box-flex1')
            else:
                self.exps = exponents(self.fnow, self.current_gradient)
                print('Running {} Code:'.format(self.opt_name))
                print('  {3:7s}   {0:{mi}s}   {1:11s}    {2:11s}'.format(
                    "i", "f", "|g|", "runtime", mi=self.len_maxiters))

    def __enter__(self):
        self.start = time.time()
        self._time = self.start
        return self

    def print_out(self, seconds):
        if seconds < 60:
            ms = (seconds % 1)*100
            self.timestring = "{s:0>2d}s{ms:0>2d}".format(
                s=int(seconds), ms=int(ms))
        else:
            m, s = divmod(seconds, 60)
            if m > 59:
                h, m = divmod(m, 60)
                if h > 23:
                    d, h = divmod(h, 24)
                    self.timestring = '{d:0>2d}d{h:0>2d}h{m:0>2d}'.format(
                        m=int(m), h=int(h), d=int(d))
                else:
                    self.timestring = '{h:0>2d}h{m:0>2d}m{s:0>2d}'.format(
                        m=int(m), s=int(s), h=int(h))
            else:
                ms = (seconds % 1)*100
                self.timestring = '{m:0>2d}m{s:0>2d}s{ms:0>2d}'.format(
                    m=int(m), s=int(s), ms=int(ms))
        if self.ipython_notebook:  # pragma: no cover
            names_vals = [['optimizer', "{:s}".format(self.opt_name)],
                          ['runtime', "{:>s}".format(self.timestring)],
                          ['evaluation', "{:>0{l}}".format(
                              self.iteration, l=self.len_maxiters)],
                          ['objective', "{: > 12.3E}".format(self.fnow)],
                          ['||gradient||',
                              "{: >+12.3E}".format(float(self.current_gradient))],
                          ['status', "{:s}".format(self.status)],
                          ]
            #message = "Lik:{:5.3E} Grad:{:5.3E} Lik:{:5.3E} Len:{!s}".format(float(m.log_likelihood()), np.einsum('i,i->', grads, grads), float(m.likelihood.variance), " ".join(["{:3.2E}".format(l) for l in m.kern.lengthscale.values]))
            html_begin = """<style type="text/css">
    .tg-opt  {font-family:"Courier New", Courier, monospace !important;padding:2px 3px;word-break:normal;border-collapse:collapse;border-spacing:0;border-color:#DCDCDC;margin:0px auto;width:100%;}
    .tg-opt td{font-family:"Courier New", Courier, monospace !important;font-weight:bold;color:#444;background-color:#F7FDFA;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#DCDCDC;}
    .tg-opt th{font-family:"Courier New", Courier, monospace !important;font-weight:normal;color:#fff;background-color:#26ADE4;border-style:solid;border-width:1px;overflow:hidden;word-break:normal;border-color:#DCDCDC;}
    .tg-opt .tg-left{font-family:"Courier New", Courier, monospace !important;font-weight:normal;text-align:left;}
    .tg-opt .tg-right{font-family:"Courier New", Courier, monospace !important;font-weight:normal;text-align:right;}
    </style>
    <table class="tg-opt">"""
            html_end = "</table>"
            html_body = ""
            for name, val in names_vals:
                html_body += "<tr>"
                html_body += "<td class='tg-left'>{}</td>".format(name)
                html_body += "<td class='tg-right'>{}</td>".format(val)
                html_body += "</tr>"
            self.text.value = html_begin + html_body + html_end
            self.progress.value = (self.iteration+1)
            #self.progresstext.value = '0/{}'.format((self.iteration+1))
            self.model_show.value = self.model._repr_html_()
        else:
            n_exps = exponents(self.fnow, self.current_gradient)
            if self.iteration - self.p_iter >= 20 * np.random.rand():
                a = self.iteration >= self.p_iter * 2.78
                b = np.any(n_exps < self.exps)
                if a or b:
                    self.p_iter = self.iteration
                    print('')
                if b:
                    self.exps = n_exps
            print('\r', end=' ')
            print('{3:}  {0:>0{mi}g}  {1:> 12e}  {2:> 12e}'.format(self.iteration, float(self.fnow), float(self.current_gradient), "{:>8s}".format(
                self.timestring), mi=self.len_maxiters), end=' ')  # print 'Iteration:', iteration, ' Objective:', fnow, '  Scale:', beta, '\r',
            sys.stdout.flush()

    def print_status(self, me, which=None):
        self.update()

        t = time.time()
        seconds = t-self.start
        #sys.stdout.write(" "*len(self.message))
        if t-self._time > 1. or seconds < .2:
            self.print_out(seconds)
            self._time = t

        self.iteration += 1

    def update(self):
        self.fnow = self.model.objective_function()
        if self.model.obj_grads is not None:
            grad = self.model.obj_grads
            self.current_gradient = np.dot(grad, grad)
        else:
            self.current_gradient = np.nan

    def finish(self, opt):  # pragma: no cover
        import warnings
        warnings.warn('Finish now automatic, deprecating', DeprecationWarning)

    def __exit__(self, type, value, traceback):
        if self.verbose:
            self.status = self.opt.status

            self.stop = time.time()
            self.model.remove_observer(self)
            self.print_out(self.stop - self.start)

            if not self.ipython_notebook:
                print()
                print('Runtime: {}'.format("{:>9s}".format(self.timestring)))
                print('Optimization status: {0}'.format(self.status))
                print()
            elif self.clear:  # pragma: no cover
                self.hor_align.close()
            else:  # pragma: no cover
                if 'conv' in self.status.lower():
                    self.progress.bar_style = 'success'
                elif self.iteration >= self.maxiters:
                    self.progress.bar_style = 'warning'
                else:
                    self.progress.bar_style = 'danger'
