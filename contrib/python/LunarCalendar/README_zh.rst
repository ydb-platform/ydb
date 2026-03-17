LunarCalendar: 一个农历-公历转换器
==================================

.. image::
  https://img.shields.io/pypi/v/LunarCalendar.svg
  :target: https://pypi.python.org/pypi/LunarCalendar
  :alt: Last stable version (PyPI)


Overview
--------

LunarCalendar 是一个农历-公历的转换器, 收录了一些在中国常见的农历(阴阳历的一种)和公历(Gregorian Calendar, 又称阳历、西历、新历)节假日。
由于韩国、日本、越南的农历与中国是相同的，只是节假日有所不同，所以支持对韩国、日本节假日和语言的扩展。

转换器支持时间段从1900-2100, 如果需要更长的时间段，利用 ``generate.html`` 生成的数据即可。
转换器的实现，参考自 `Lunar-Solar-Calendar-Converter <https://github.com/isee15/Lunar-Solar-Calendar-Converter>`_.


Features
--------

* 原始数据精准, 通过了微软 ``ChineseLunisolarCalendar`` 类的比对
* 节假日扩展与语言支持非常便捷, 支持简体和繁体
* 收录了农历节假日, 如: 中秋/端午/除夕/重阳
* 收录了每年不固定日期的节假日, 如: 母亲节(每年5月第2个星期日)
* 加入了农历+公历的合法性检查
* 支持二十四节气


Install
-------

LunarCalendar can be installed from the PyPI with ``easy_install``::

   $ easy_install LunarCalendar

Or pip::

   $ pip install LunarCalendar

如果在安装时遇到诸如此类的错误: ``command 'gcc' failed with exit status 1 while installing ephem``, 可能你需要先安装 ``python-devel`` 库.
For CentOS::

   $ yum install python-devel

For Ubuntu::

   $ apt-get install python-dev



Console Commands
----------------

命令行可查找每年特定节日的日期，默认查找今年。节日支持别名查询:

.. code-block:: console

    $ lunar-find 重阳
    重阳节 on 2018: 2018-10-17

    $ lunar-find 重陽節
    重阳节 on 2018: 2018-10-17

    $ lunar-find 登高节 2019
    重阳节 on 2019: 2019-10-07

按照时间升序打印对应年份的所有内置日期, 所有节日, 所有节气:

.. code-block:: console

    $ lunar-find all 2019
    $ lunar-find festival 2012
    $ lunar-find 节日 2012
    $ lunar-find solarterm
    $ lunar-find 节气


Quickstart
----------

公历转农历:

.. code-block:: python

    import datetime
    from lunarcalendar import Converter, Solar, Lunar, DateNotExist

    solar = Solar(2018, 1, 1)
    print(solar)
    lunar = Converter.Solar2Lunar(solar)
    print(lunar)
    solar = Converter.Lunar2Solar(lunar)
    print(solar)
    print(solar.to_date(), type(solar.to_date()))

农历转公历:

.. code-block:: python

    lunar = Lunar(2018, 2, 30, isleap=False)
    print(lunar)
    solar = Converter.Lunar2Solar(lunar)
    print(solar)
    lunar = Converter.Solar2Lunar(solar)
    print(lunar)
    print(lunar.to_date(), type(lunar.to_date()))
    print(Lunar.from_date(datetime.date(2018, 4, 15)))

日期合法性检查, 农历和公历都起作用, 如: 农历闰月2018-2-15是不存在的，但农历闰月2012-4-4是存在的:

.. code-block:: python

    Lunar(2012, 4, 4, isleap=True)  # date(2012, 5, 24)
    try:
        lunar = Lunar(2018, 2, 15, isleap=True)
    except DateNotExist:
        print(traceback.format_exc())

打印收录的节假日, 支持中文、英文输出，其他语言需要扩展(欢迎fork & pull-request):

.. code-block:: python

    from lunarcalendar.festival import festivals

    # print festivals, using English or Chinese
    print("----- print all festivals on 2018 in chinese: -----")
    for fest in festivals:
        print(fest.get_lang('zh'), fest(2018))

    print("----- print all festivals on 2017 in english: -----")
    for fest in festivals:
        print(fest.get_lang('en'), fest(2017))

输出:

.. code-block:: shell

    ......
    母亲节 2018-05-13
    父亲节 2018-06-17
    中秋节 2018-09-24
    感恩节 2018-11-22
    重阳节 2018-10-17
    春节 2018-02-16
    中元节 2018-08-25
    七夕节 2018-08-17
    腊八节 2019-01-13
    清明节 2018-04-05
    除夕 2019-02-04
    寒衣节 2018-11-08
    元宵节 2018-03-02
    龙抬头 2018-03-18
    端午节 2018-06-18
    ......


Contribution
------------

收录节日的标准:

* 在对应国家中常见的节假日，如: 圣诞节、万圣节等。
* 农历节假日
* 公历节假日，但每年时间不固定，如: 母亲节、复活节等。

目前只支持中文和英文，如果要支持韩文、日文的节假日，需要在 ``lunarcalendar/festival.py`` 中添加对应的语言和节假日。

一些罕见的节假日可能未被收录, `欢迎补充 <https://github.com/wolfhong/LunarCalendar/issues>`_ .


About
-----

* `Homepage <http://github.com/wolfhong/LunarCalendar>`_
* `PyPI <https://pypi.python.org/pypi/LunarCalendar>`_
* `Issue tracker <https://github.com/wolfhong/LunarCalendar/issues?status=new&status=open>`_
