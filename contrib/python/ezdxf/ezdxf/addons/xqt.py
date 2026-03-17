#  Copyright (c) 2021, Manfred Moitzi
#  License: MIT License
# mypy: ignore_errors=True
from ezdxf._options import DRAWING_ADDON, options

# Qt compatibility layer: all Qt imports from ezdxf.addons.xqt
PYSIDE6 = False
TRY_PYSIDE6 = options.get_bool(DRAWING_ADDON, "try_pyside6", True)
PYQT5 = False
TRY_PYQT5 = options.get_bool(DRAWING_ADDON, "try_pyqt5", True)

if TRY_PYSIDE6:
    try:
        from PySide6 import QtGui, QtCore, QtWidgets
        from PySide6.QtWidgets import (
            QFileDialog,
            QInputDialog,
            QMessageBox,
            QTableView,
            QTreeView,
            QListView,
        )
        from PySide6.QtCore import (
            QAbstractTableModel,
            QStringListModel,
            QFileSystemWatcher,
            QModelIndex,
            QPointF,
            QSettings,
            QSize,
            Qt,
            Signal,
            Slot,
        )
        from PySide6.QtGui import (
            QAction,
            QColor,
            QPainterPath,
            QStandardItem,
            QStandardItemModel,
        )

        PYSIDE6 = True
        print("using Qt binding: PySide6")
    except ImportError:
        pass

# PyQt5 is just a fallback
if TRY_PYQT5 and not PYSIDE6:
    try:
        from PyQt5 import QtGui, QtCore, QtWidgets
        from PyQt5.QtCore import pyqtSignal as Signal
        from PyQt5.QtCore import pyqtSlot as Slot
        from PyQt5.QtWidgets import (
            QAction,
            QFileDialog,
            QInputDialog,
            QMessageBox,
            QTableView,
            QTreeView,
            QListView,
        )
        from PyQt5.QtCore import (
            QAbstractTableModel,
            QStringListModel,
            QFileSystemWatcher,
            QModelIndex,
            QPointF,
            QSettings,
            QSize,
            Qt,
        )
        from PyQt5.QtGui import (
            QColor,
            QPainterPath,
            QStandardItem,
            QStandardItemModel,
        )

        PYQT5 = True
        print("using Qt binding: PyQt5")
    except ImportError:
        pass

if not (PYSIDE6 or PYQT5):
    raise ImportError("no Qt binding found, tried PySide6 and PyQt5")
