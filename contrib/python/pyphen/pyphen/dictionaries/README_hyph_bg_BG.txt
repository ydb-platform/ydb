  Това е пакет за сричкопренасяне за OpenOffice.org. Може
да свалите OpenOffice.org от http://openoffice.org/.
  Пакетът се поддържа заедно с проекта bgOffice. За повече
информация, прочетете файла README.bgOffice, който идва с
този пакет или посетете страницата на проекта на адрес:
http://bgoffice.sourceforge.net.

bghyphen.tex -- TeX hyphenation patterns for Bulgarian
Copyright 2000 Anton Zinoviev <anton@lml.bas.bg>

Translation to ALTLinux hyphenator format (for use in
OpenOffice.org) July 2002 Borislav Mitev <morbid_viper@mail.bg>



              Указания за инсталиране и настройка


1. Копирате файла hyph_bg_BG.dic в директорията:

  ..\OpenOffice.org\share\dict\ooo

  Ако сте инсталирали проверка на правописа на американски
английски, то в тази директория трябва да се намират файловете
en_US.aff, en_US.dic и dictionary.lst.


2. Отваряте файла dictionary.lst с текстов редактор и добавяте
следния ред на края му:

  HYPH bg BG hyph_bg_BG


3. Стартирате OpenOffice и осъществявате следните настройки:
Tools/Options/Language Settings:
- В Languages/Default languages for documents трябва да се
посочи Bulgarian;
- В Writing Aids/Available language modules/Edit за
Language/Bulgarian трябва да се е появил под реда Hyphenation
нов ред с етикет ALTLinux LibHnj Hyphenator, който трябва да се
избере.


4. За да направи самото сричкопренасяне върху текста от
Tools/Hyphenation...
- По подразбиране сричкопренасянето е включено на ръчен режим,
т.е. ще пита за всяка една дума как да я пренесе, което за
големи текстове не много удобно. За да се пусне в автоматичен
трябва да се избере Hyphenate without inquiry от
Tools/Options/Language Settings/Writing Aids/Options;
- При повторно сричкопренасяне, което обикновено се налага при
промяна на текста или формата, тиретата остават на старите си
места и трябва да се махнат ръчно. Надявам се това да бъде
променено в по-следващите версии.


  Всяка помощ е добра дошла. Търсят се доброволци да помагат.


  OOo-hyph-bg
  Авторски права (C) 2001 Антон Зиновиев <anton@lml.bas.bg>
                          Борислав Митев <morbid_viper@mail.bg>
  Поддържа се от Радостин Раднев <radnev@yahoo.com>

  Получавате тази програма БЕЗ КАКВИТО И ДА Е ГАРАНЦИИ. Това е
свободна програма и, ако желаете, можете да я разпространявате
при определени условия. За подробности прочетете файла
COPYING.BULGARIAN, който идва с този пакет.


  OOo-hyph-bg
  Copyright (C) 2001 Anton Zinoviev <anton@lml.bas.bg>
                     Borislav Mitev <morbid_viper@mail.bg>
  Maintained by Radostin Radnev <radnev@yahoo.com>

  This program comes with ABSOLUTELY NO WARRANTY. This is free
software, and you are welcome to redistribute it under certain
conditions. For details read file COPYING that comes with this
package.
