# cae.py - functions for handling Spanish CAE number
# coding: utf-8
#
# Copyright (C) 2024 Quique Porta
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301 USA

"""CAE (Código de Actividad y Establecimiento, Spanish activity establishment code).

The Código de Actividad y Establecimiento (CAE) is assigned by the Spanish
Tax Agency companies or establishments that carry out activities related to
products subject to excise duty. It identifies an activity and the
establishment in which it is carried out.

The number consists of 13 characters where the sixth and seventh characters
identify the managing office in which the territorial registration is carried
out and the eighth and ninth characters identify the activity that takes
place.


More information:

* https://www.boe.es/boe/dias/2006/12/28/pdfs/A46098-46100.pdf
* https://www2.agenciatributaria.gob.es/L/inwinvoc/es.aeat.dit.adu.adce.cae.cw.AccW?fAccion=consulta

>>> validate('ES00008V1488Q')
'ES00008V1488Q'
>>> validate('00008V1488')  # invalid check length
Traceback (most recent call last):
    ...
InvalidLength: ...
>>> is_valid('ES00008V1488Q')
True
>>> is_valid('00008V1488')
False
>>> compact('ES00008V1488Q')
'ES00008V1488Q'
"""

from __future__ import annotations

from stdnum.exceptions import *
from stdnum.util import clean, isdigits


_OFFICES = {
    '01',  # Álava
    '02',  # Albacete
    '03',  # Alicante
    '04',  # Almería
    '05',  # Ávila
    '06',  # Badajoz
    '07',  # Illes Balears
    '08',  # Barcelona
    '09',  # Burgos
    '10',  # Cáceres
    '11',  # Cádiz
    '12',  # Castellón
    '13',  # Ciudad Real
    '14',  # Córdoba
    '15',  # A Coruña
    '16',  # Cuenca
    '17',  # Girona
    '18',  # Granada
    '19',  # Guadalajara
    '20',  # Guipúzcoa
    '21',  # Huelva
    '22',  # Huesca
    '23',  # Jaén
    '24',  # León
    '25',  # Lleida
    '26',  # La Rioja
    '27',  # Lugo
    '28',  # Madrid
    '29',  # Málaga
    '30',  # Murcia
    '31',  # Navarra
    '32',  # Ourense
    '33',  # Oviedo
    '34',  # Palencia
    '35',  # Las Palmas
    '36',  # Pontevedra
    '37',  # Salamanca
    '38',  # Santa Cruz de Tenerife
    '39',  # Santander
    '40',  # Segovia
    '41',  # Sevilla
    '42',  # Soria
    '43',  # Tarragona
    '44',  # Teruel
    '45',  # Toledo
    '46',  # Valencia
    '47',  # Valladolid
    '48',  # Bizcaia
    '49',  # Zamora
    '50',  # Zaragoza
    '51',  # Cartagena
    '52',  # Gijón
    '53',  # Jerez de la Frontera
    '54',  # Vigo
    '55',  # Ceuta
    '56',  # Melilla
}

_ACTIVITY_KEYS = {
    'A1',  # Fábricas de alcohol
    'B1',  # Fábricas de bebidas derivadas
    'B9',  # Elaboradores de productos intermedios distintos de los comprendidos en B-0
    'B0',  # Elaboradores de productos intermedios en régimen especial
    'BA',  # Fábricas de bebidas alcohólicas
    'C1',  # Fábricas de cerveza
    'DA',  # Destiladores artesanales
    'EC',  # Fábricas de extractos y concentrados alcohólicos
    'F1',  # Elaboradores de otras bebidas fermentadas
    'V1',  # Elaboradores de vinos
    'A7',  # Depósitos fiscales de alcohol
    'AT',  # Almacenes fiscales de alcohol
    'B7',  # Depósitos fiscales de bebidas derivadas
    'BT',  # Almacenes fiscales de bebidas alcohólicas
    'C7',  # Depósitos fiscales de cerveza
    'DB',  # Depósitos fiscales de bebidas alcohólicas
    'E7',  # Depósitos fiscales de extractos y concentrados alcohólicos exclusivamente
    'M7',  # Depósitos fiscales de productos intermedios
    'OA',  # Operadores registrados de alcohol
    'OB',  # Operadores registrados de bebidas alcohólicas
    'OE',  # Operadores registrados de extractos y concentrados alcohólicos
    'OV',  # Operadores registrados de vinos y de otras bebidas fermentadas
    'V7',  # Depósitos fiscales de vinos y de otras bebidas fermentadas
    'B6',  # Plantas embotelladoras de bebidas derivadas
    'A2',  # Centros de investigación
    'A6',  # Usuarios de alcohol totalmente desnaturalizado
    'A9',  # Industrias de especialidades farmacéuticas
    'A0',  # Centros de atención médica
    'AC',  # Usuarios con derecho a devolución
    'AV',  # Usuarios de alcohol parcialmente desnaturalizado con desnaturalizante general
    'AW',  # Usuarios de alcohol parcialmente desnaturalizado con desnaturalizante especial
    'AX',  # Fábricas de vinagre
    'H1',  # Refinerías de crudo de petróleo
    'H2',  # Fábricas de biocarburante, consistente en alcohol etílico
    'H4',  # Fábricas de biocarburante o biocombustible con sistente en biodiesel
    'H6',  # Fábricas de biocarburante o biocombustible con sistente en alcohol metílico
    'H9',  # Industrias extractoras de gas natural y otros productos gaseosos
    'H0',  # Las demás industrias que obtienen productos gravados
    'HD',  # Industrias o establecimientos que someten productos a un tratamiento definido o,
           # Previa solicitud, a una transformación química
    'HH',  # Industrias extractoras de crudo de petróleo
    'H7',  # Depósitos fiscales de hidrocarburos
    'H8',  # Depósitos fiscales exclusivamente de biocarburantes
    'HB',  # Obtención accesoria de productos sujetos alimpuesto
    'HF',  # Almacenes fiscales para el suministro directo a instalaciones fijas
    'HI',  # Depósitos fiscales exclusivamente para la distribución de querosenos y gasolinas de aviación
    'HJ',  # Depósitos fiscales exclusivamente de productos de la tarifa segunda
    'HK',  # Instalaciones de venta de gas natural con tipo general y tipo reducido
    'HL',  # Almacenes fiscales exclusivamente de productos de la tarifa segunda
    'HM',  # Almacenes fiscales para la gestión de aceites usados destinados a su utilización como  combustibles
    'HN',  # Depósitos fiscales constituidos por una red de oleoductos
    'HT',  # Almacenes fiscales para el comercio al por mayor de hidrocarburos
    'HU',  # Almacenes fiscales constituidos por redes de transporte o distribución de gas natural
    'HV',  # Puntos de suministro marítimo de gasóleo
    'HX',  # Depósitos fiscales constituidos por una red de gasoductos
    'HZ',  # Detallistas de gasóleo
    'OH',  # Operadores registrados de hidrocarburos
    'HA',  # Titulares de aeronaves que utilizan instalaciones privadas
    'HC',  # Explotaciones industriales y proyectos piloto con derecho a devolución
    'HE',  # Los demás usuarios con derecho a exención
    'HP',  # Inyección en altos hornos
    'HQ',  # Construcción, modificación, pruebas y mantenimiento de aeronaves y embarcaciones
    'HR',  # Producción de electricidad en centrales eléctricas o producción de electricidad o
           # cogeneración de electricidad y de calor en centrales combinadas
    'HS',  # Transporte por ferrocarril
    'HW',  # Consumidores de combustibles y carburantes a tipo reducido (artículos 106.4 y 108
           # del Reglamento de los Impuestos Especiales)
    'T1',  # Fábricas de labores del tabaco
    'OT',  # Operadores registrados de labores del tabaco
    'T7',  # Depósitos fiscales de labores del tabaco
    'TT',  # Almacenes fiscales de labores del tabaco
    'L1',  # Fábricas de electricidad en régimen ordinario
    'L2',  # Generadores o conjunto de generadores de potencia total superior a 100 kilovatios
    'L0',  # Fábricas de electricidad en régimen especial
    'L3',  # Los demás sujetos pasivos
    'L7',  # Depósitos fiscales de electricidad
    'AF',  # Almacenes fiscales de bebidas alcohólicas y de labores del tabaco
    'DF',  # Depósitos fiscales de bebidas alcohólicas y de labores del tabaco
    'DM',  # Depósitos fiscales de bebidas alcohólicas y de labores del tabaco situados en
           # puertos y aeropuertos y que funcionen exclusivamente como establecimientos minoristas
    'DP',  # Depósitos fiscales para el suministro de bebidas alcohólicas y de labores del
           # tabaco para consumo o venta a bordo de buques y/o aeronaves
    'OR',  # Operadores registrados de bebidas alcohólicas y de labores del tabaco
    'PF',  # Industrias o usuarios en régimen de perfeccionamiento fiscal
    'RF',  # Representantes fiscales
    'VD',  # Empresas de ventas a distancia
}


def compact(number: str) -> str:
    """Convert the number to the minimal representation. This strips the
    number of any valid separators and removes surrounding whitespace."""
    return clean(number).upper().strip()


def validate(number: str) -> str:
    """Check if the number provided is a valid CAE number. This checks the
    length and formatting."""
    number = compact(number)
    if len(number) != 13:
        raise InvalidLength()
    if number[:2] != 'ES':
        raise InvalidFormat()
    if number[2:5] != '000':
        raise InvalidFormat()
    if number[5:7] not in _OFFICES:
        raise InvalidFormat()
    if number[7:9] not in _ACTIVITY_KEYS:
        raise InvalidFormat()
    if not isdigits(number[9:12]):
        raise InvalidFormat()
    if not number[12].isalpha():
        raise InvalidFormat()
    return number


def is_valid(number: str) -> bool:
    """Check if the number provided is a valid CAE number. This checks the
    length and formatting."""
    try:
        return bool(validate(number))
    except ValidationError:
        return False
