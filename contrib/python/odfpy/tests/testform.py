#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2008-2010 Søren Roug, European Environment Agency
#
# This is free software.  You may redistribute it under the terms
# of the Apache license and the GNU General Public License Version
# 2 or at your option any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public
# License along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
#
# Contributor(s):
#

import unittest, os, os.path
from odf.opendocument import OpenDocumentSpreadsheet
import odf.table
import odf.office
import odf.form
import odf.draw

class TestForm(unittest.TestCase):
    def test_ooo_ns(self):
        """ Check that ooo exists in namespace declarations """
        calcdoc = OpenDocumentSpreadsheet()
        table = odf.table.Table(name="Costs")
        forms = odf.office.Forms()
        form = odf.form.Form(
           controlimplementation="ooo:com.sun.star.form.component.Form")
        lb = odf.form.Listbox(
           controlimplementation="ooo:com.sun.star.form.component.ListBox", dropdown="true", id="control1")
        form.addElement(lb)
        forms.addElement(form)
        table.addElement(forms)

        # One empty line
        tr = odf.table.TableRow()
        table.addElement(tr)

        tr = odf.table.TableRow()
        # One empty cell
        cell = odf.table.TableCell()
        tr.addElement(cell)

        cell = odf.table.TableCell()

        draw = odf.draw.Control(
        control="control1", height="0.1126in", width="0.798in",
        x="0.0303in", y="0.0205in", endcelladdress="Costs.B2",
        endx="0.8283in", endy="0.1331in")

        cell.addElement(draw)
        tr.addElement(cell)
        table.addElement(tr)

        calcdoc.spreadsheet.addElement(table)
        result = calcdoc.contentxml() # contentxml() is supposed to yeld a bytes
        self.assertNotEqual(-1, result.find(b'''xmlns:ooo="http://openoffice.org/2004/office"'''))

    def test_form_controls(self):
        odf.form.Button(id="Button")
        odf.form.Checkbox(id="Checkbox")
        odf.form.Combobox(id="Combobox")
        odf.form.Date(id="Date")
        odf.form.File(id="File")
        odf.form.FixedText(id="FixedText")
        odf.form.FormattedText(id="FormattedText")
        odf.form.Frame(id="Frame")
        odf.form.GenericControl(id="GenericControl")
        odf.form.Grid(id="Grid")
        odf.form.Hidden(id="Hidden")
        odf.form.Image(id="Image")
        odf.form.ImageFrame(id="ImageFrame")
        odf.form.Listbox(id="Listbox")
        odf.form.Number(id="Number")
        odf.form.Password(id="Password")
        odf.form.Radio(id="Radio")
        odf.form.Text(id="Text")
        odf.form.Textarea(id="Textarea")
        odf.form.Time(id="Time")
        odf.form.ValueRange(id="ValueRange")

if __name__ == '__main__':
    unittest.main()
