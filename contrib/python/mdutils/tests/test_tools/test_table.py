# Python
#
# This module implements tests for Table class.
#
# This file is part of mdutils. https://github.com/didix21/mdutils
#
# MIT License: (C) 2018 Dídac Coll

from unittest import TestCase
from mdutils.tools.Table import Table
from mdutils.mdutils import MdUtils

__author__ = "didix21"
__project__ = "MdUtils"


class TestTable(TestCase):
    def test_create_centered_table(self):
        md_file = MdUtils("file_name")
        table = Table()
        result_table = (
            "\n|**Test**|**Descripción**|**Estado**|\n| :---: | :---: | :---: "
            '|\n|Test 1|Carga de configuración correcta|<font color="green">OK</font>|\n'
            '|Test 2|Lectura de Configuración|<font color="red">NOK</font>|\n'
            '|Test 3|Lectura de Soporte|<font color="green">OK</font>|\n'
            '|Test 4|Modificación de entradas y lectura de salidas de cantón|<font color="green">'
            "OK</font>|"
            '\n|Test 5|Lectura de estados de Pedal de Rearme y Aviso|<font color="green">OK</font>|\n'
            '|Test 6|Actualización de datos de unidades de vía|<font color="green">OK</font>|\n'
            '|Test 7|Fallos en carga de configuración - Campo IdApp Erróneo|<font color="green">'
            "OK</font>|"
            "\n"
            "|Test 8|Fallos en carga de configuración - Campo VersTAbla Erróneo"
            '|<font color="red">NOK</font>|'
            '\n|Test 9|Fallos en carga de configuración - Campo IdUc Erróneo|<font color="red">'
            "NOK</font>|"
            "\n|Test 10|Fallos en carga de configuración - Campo Addresses Erróneo"
            '|<font color="red">NOK</font>|\n'
            "|Test 11|Fallos en carga de configuración - Campo NumTc Erróneo"
            '|<font color="red">NOK</font>|\n'
            "|Test 12|Fallos en carga de configuración - Campo NumUv Erróneo"
            '|<font color="red">NOK</font>|\n'
            '|Test 13|Fallos en carga de configuración - Campo CRC Erróneo|<font color="red">NOK</font>|\n'
        )

        text_array = [
            "**Test**",
            "**Descripción**",
            "**Estado**",
            "Test 1",
            "Carga de configuración correcta",
            md_file.textUtils.text_color("OK", "green"),
            "Test 2",
            "Lectura de Configuración",
            md_file.textUtils.text_color("NOK", "red"),
            "Test 3",
            "Lectura de Soporte",
            md_file.textUtils.text_color("OK", "green"),
            "Test 4",
            "Modificación de entradas y lectura de salidas de cantón",
            md_file.textUtils.text_color("OK", "green"),
            "Test 5",
            "Lectura de estados de Pedal de Rearme y Aviso",
            md_file.textUtils.text_color("OK", "green"),
            "Test 6",
            "Actualización de datos de unidades de vía",
            md_file.textUtils.text_color("OK", "green"),
            "Test 7",
            "Fallos en carga de configuración - Campo IdApp Erróneo",
            md_file.textUtils.text_color("OK", "green"),
            "Test 8",
            "Fallos en carga de configuración - Campo VersTAbla Erróneo",
            md_file.textUtils.text_color("NOK", "red"),
            "Test 9",
            "Fallos en carga de configuración - Campo IdUc Erróneo",
            md_file.textUtils.text_color("NOK", "red"),
            "Test 10",
            "Fallos en carga de configuración - Campo Addresses Erróneo",
            md_file.textUtils.text_color("NOK", "red"),
            "Test 11",
            "Fallos en carga de configuración - Campo NumTc Erróneo",
            md_file.textUtils.text_color("NOK", "red"),
            "Test 12",
            "Fallos en carga de configuración - Campo NumUv Erróneo",
            md_file.textUtils.text_color("NOK", "red"),
            "Test 13",
            "Fallos en carga de configuración - Campo CRC Erróneo",
            md_file.textUtils.text_color("NOK", "red"),
        ]

        self.assertEqual(
            table.create_table(
                columns=3, rows=14, text=text_array, text_align="center"
            ),
            result_table,
        )

    def test_create_default_table(self):
        md_file = MdUtils("file_name")
        table = Table()
        result_table = (
            "\n|**Test**|**Descripción**|**Estado**|\n| --- | --- | --- "
            '|\n|Test 1|Carga de configuración correcta|<font color="green">OK</font>|\n'
            '|Test 2|Lectura de Configuración|<font color="red">NOK</font>|\n'
            '|Test 3|Lectura de Soporte|<font color="green">OK</font>|\n'
            '|Test 4|Modificación de entradas y lectura de salidas de cantón|<font color="green">'
            "OK</font>|"
            '\n|Test 5|Lectura de estados de Pedal de Rearme y Aviso|<font color="green">OK</font>|\n'
            '|Test 6|Actualización de datos de unidades de vía|<font color="green">OK</font>|\n'
            '|Test 7|Fallos en carga de configuración - Campo IdApp Erróneo|<font color="green">'
            "OK</font>|"
            "\n"
            "|Test 8|Fallos en carga de configuración - Campo VersTAbla Erróneo"
            '|<font color="red">NOK</font>|'
            '\n|Test 9|Fallos en carga de configuración - Campo IdUc Erróneo|<font color="red">'
            "NOK</font>|"
            "\n|Test 10|Fallos en carga de configuración - Campo Addresses Erróneo"
            '|<font color="red">NOK</font>|\n'
            "|Test 11|Fallos en carga de configuración - Campo NumTc Erróneo"
            '|<font color="red">NOK</font>|\n'
            "|Test 12|Fallos en carga de configuración - Campo NumUv Erróneo"
            '|<font color="red">NOK</font>|\n'
            '|Test 13|Fallos en carga de configuración - Campo CRC Erróneo|<font color="red">NOK</font>|\n'
        )

        text_array = [
            "**Test**",
            "**Descripción**",
            "**Estado**",
            "Test 1",
            "Carga de configuración correcta",
            md_file.textUtils.text_color("OK", "green"),
            "Test 2",
            "Lectura de Configuración",
            md_file.textUtils.text_color("NOK", "red"),
            "Test 3",
            "Lectura de Soporte",
            md_file.textUtils.text_color("OK", "green"),
            "Test 4",
            "Modificación de entradas y lectura de salidas de cantón",
            md_file.textUtils.text_color("OK", "green"),
            "Test 5",
            "Lectura de estados de Pedal de Rearme y Aviso",
            md_file.textUtils.text_color("OK", "green"),
            "Test 6",
            "Actualización de datos de unidades de vía",
            md_file.textUtils.text_color("OK", "green"),
            "Test 7",
            "Fallos en carga de configuración - Campo IdApp Erróneo",
            md_file.textUtils.text_color("OK", "green"),
            "Test 8",
            "Fallos en carga de configuración - Campo VersTAbla Erróneo",
            md_file.textUtils.text_color("NOK", "red"),
            "Test 9",
            "Fallos en carga de configuración - Campo IdUc Erróneo",
            md_file.textUtils.text_color("NOK", "red"),
            "Test 10",
            "Fallos en carga de configuración - Campo Addresses Erróneo",
            md_file.textUtils.text_color("NOK", "red"),
            "Test 11",
            "Fallos en carga de configuración - Campo NumTc Erróneo",
            md_file.textUtils.text_color("NOK", "red"),
            "Test 12",
            "Fallos en carga de configuración - Campo NumUv Erróneo",
            md_file.textUtils.text_color("NOK", "red"),
            "Test 13",
            "Fallos en carga de configuración - Campo CRC Erróneo",
            md_file.textUtils.text_color("NOK", "red"),
        ]

        self.assertEqual(
            table.create_table(columns=3, rows=14, text=text_array), result_table
        )

    def test_column_row_does_not_much_text_length_array(self):
        md_file = MdUtils("file_name")
        table = Table()
        text_array = [
            "**Test**",
            "**Descripción**",
            "**Estado**",
            "Test 1",
            "Carga de configuración correcta",
            md_file.textUtils.text_color("OK", "green"),
            "Test 2",
            "Lectura de Configuración",
            md_file.textUtils.text_color("NOK", "red"),
            "Test 3",
            "Lectura de Soporte",
            md_file.textUtils.text_color("OK", "green"),
        ]

        self.assertRaises(ValueError, table.create_table, 3, 14, text_array)

    def test_invalid_text_align(self):
        md_file = MdUtils("file_name")
        table = Table()
        text_array = [
            "**Test**",
            "**Descripción**",
            "**Estado**",
            "Test 1",
            "Carga de configuración correcta",
            md_file.textUtils.text_color("OK", "green"),
            "Test 2",
            "Lectura de Configuración",
            md_file.textUtils.text_color("NOK", "red"),
            "Test 3",
            "Lectura de Soporte",
            md_file.textUtils.text_color("OK", "green"),
        ]

        self.assertRaises(
            ValueError, table.create_table, 3, 14, text_array, "invalid_align"
        )

    def test_create_table_array_basic(self):
        """Test basic functionality of create_table_array method."""
        table = Table()
        data = [
            ["Header 1", "Header 2", "Header 3"],
            ["Row 1 Col 1", "Row 1 Col 2", "Row 1 Col 3"],
            ["Row 2 Col 1", "Row 2 Col 2", "Row 2 Col 3"]
        ]
        expected_result = (
            "\n|Header 1|Header 2|Header 3|\n| --- | --- | --- |\n"
            "|Row 1 Col 1|Row 1 Col 2|Row 1 Col 3|\n"
            "|Row 2 Col 1|Row 2 Col 2|Row 2 Col 3|\n"
        )
        
        result = table.create_table_array(data=data)
        self.assertEqual(result, expected_result)

    def test_create_table_array_with_center_alignment(self):
        """Test create_table_array with center text alignment."""
        table = Table()
        data = [
            ["Header 1", "Header 2"],
            ["Data 1", "Data 2"]
        ]
        expected_result = (
            "\n|Header 1|Header 2|\n| :---: | :---: |\n"
            "|Data 1|Data 2|\n"
        )
        
        result = table.create_table_array(data=data, text_align="center")
        self.assertEqual(result, expected_result)

    def test_create_table_array_with_left_alignment(self):
        """Test create_table_array with left text alignment."""
        table = Table()
        data = [
            ["Header 1", "Header 2"],
            ["Data 1", "Data 2"]
        ]
        expected_result = (
            "\n|Header 1|Header 2|\n| :--- | :--- |\n"
            "|Data 1|Data 2|\n"
        )
        
        result = table.create_table_array(data=data, text_align="left")
        self.assertEqual(result, expected_result)

    def test_create_table_array_with_right_alignment(self):
        """Test create_table_array with right text alignment."""
        table = Table()
        data = [
            ["Header 1", "Header 2"],
            ["Data 1", "Data 2"]
        ]
        expected_result = (
            "\n|Header 1|Header 2|\n| ---: | ---: |\n"
            "|Data 1|Data 2|\n"
        )
        
        result = table.create_table_array(data=data, text_align="right")
        self.assertEqual(result, expected_result)

    def test_create_table_array_with_mixed_alignment(self):
        """Test create_table_array with mixed text alignment per column."""
        table = Table()
        data = [
            ["Left", "Center", "Right"],
            ["L1", "C1", "R1"],
            ["L2", "C2", "R2"]
        ]
        expected_result = (
            "\n|Left|Center|Right|\n| :--- | :---: | ---: |\n"
            "|L1|C1|R1|\n|L2|C2|R2|\n"
        )
        
        result = table.create_table_array(data=data, text_align=["left", "center", "right"])
        self.assertEqual(result, expected_result)

    def test_create_table_array_uneven_rows(self):
        """Test create_table_array with rows of different lengths."""
        table = Table()
        data = [
            ["Header 1", "Header 2", "Header 3"],
            ["Row 1 Col 1", "Row 1 Col 2"],  # Missing third column
            ["Row 2 Col 1"]  # Missing second and third columns
        ]
        expected_result = (
            "\n|Header 1|Header 2|Header 3|\n| --- | --- | --- |\n"
            "|Row 1 Col 1|Row 1 Col 2||\n"
            "|Row 2 Col 1|||\n"
        )
        
        result = table.create_table_array(data=data)
        self.assertEqual(result, expected_result)

    def test_create_table_array_mixed_data_types(self):
        """Test create_table_array with mixed data types (converts to strings)."""
        table = Table()
        data = [
            ["Name", "Age", "Score", "Active"],
            ["Alice", 25, 95.5, True],
            ["Bob", 30, 87.2, False],
            ["Charlie", None, 92.0, True]
        ]
        expected_result = (
            "\n|Name|Age|Score|Active|\n| --- | --- | --- | --- |\n"
            "|Alice|25|95.5|True|\n"
            "|Bob|30|87.2|False|\n"
            "|Charlie|None|92.0|True|\n"
        )
        
        result = table.create_table_array(data=data)
        self.assertEqual(result, expected_result)

    def test_create_table_array_empty_data(self):
        """Test create_table_array with empty data."""
        table = Table()
        data = []
        expected_result = ""
        
        result = table.create_table_array(data=data)
        self.assertEqual(result, expected_result)

    def test_create_table_array_single_row(self):
        """Test create_table_array with a single row."""
        table = Table()
        data = [["Single", "Row", "Data"]]
        expected_result = (
            "\n|Single|Row|Data|\n| --- | --- | --- |\n"
        )
        
        result = table.create_table_array(data=data)
        self.assertEqual(result, expected_result)

    def test_create_table_array_single_column(self):
        """Test create_table_array with a single column."""
        table = Table()
        data = [["Header"], ["Row 1"], ["Row 2"]]
        expected_result = (
            "\n|Header|\n| --- |\n"
            "|Row 1|\n|Row 2|\n"
        )
        
        result = table.create_table_array(data=data)
        self.assertEqual(result, expected_result)

    def test_create_table_array_with_special_characters(self):
        """Test create_table_array with special characters that need escaping."""
        table = Table()
        data = [
            ["Header|With|Pipes", "Normal Header"],
            ["Data|With|Pipes", "Normal Data"]
        ]
        expected_result = (
            "\n|Header\\|With\\|Pipes|Normal Header|\n| --- | --- |\n"
            "|Data\\|With\\|Pipes|Normal Data|\n"
        )
        
        result = table.create_table_array(data=data)
        self.assertEqual(result, expected_result)

    def test_create_table_array_rows_with_empty_lists(self):
        """Test create_table_array with some empty rows."""
        table = Table()
        data = [
            ["Header 1", "Header 2"],
            [],  # Empty row
            ["Row 2 Col 1", "Row 2 Col 2"]
        ]
        expected_result = (
            "\n|Header 1|Header 2|\n| --- | --- |\n"
            "|||\n"
            "|Row 2 Col 1|Row 2 Col 2|\n"
        )
        
        result = table.create_table_array(data=data)
        self.assertEqual(result, expected_result)

    def test_create_table_array_with_partial_alignment_list(self):
        """Test create_table_array with alignment list shorter than number of columns."""
        table = Table()
        data = [
            ["Col 1", "Col 2", "Col 3"],
            ["Data 1", "Data 2", "Data 3"]
        ]
        expected_result = (
            "\n|Col 1|Col 2|Col 3|\n| :--- | :---: | --- |\n"
            "|Data 1|Data 2|Data 3|\n"
        )
        
        result = table.create_table_array(data=data, text_align=["left", "center"])
        self.assertEqual(result, expected_result)
