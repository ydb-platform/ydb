class Cell {
  constructor(text, onUpdate) {
    this.elem = undefined;
    this.text = text;
    this.header = false;
    this.onUpdate = onUpdate;
    this.colspan = 1;
    this.rowspan = 1;
  }

  setRowspan(rowspan) {
    this.rowspan = rowspan;
    this._render();
  }

  setColspan(colspan) {
    this.colspan = colspan;
    this._render();
  }

  setHeader(isHeader) {
    this.header = isHeader;
    var el = this.elem;
    var newone = this.header ? $("<th>") : $("<td>");
    newone.addClass(el.attr("class"));
    el.before(newone);
    el.remove();
    this.elem = newone;
    this._render();
  }

  setElem(elem) {
    if (this.elem != undefined) {
      elem.addClass(this.elem.attr("class"));
    }
    this.elem = elem;
    this._render();
  }

  setText(text, silent = false) {
    this.text = text;
    this._render();
    if (!silent) {
      this.onUpdate(this);
    }
  }

  _render() {
    if (this.elem) {
      this.elem.attr("colspan", this.colspan);
      this.elem.attr("rowspan", this.rowspan);
      this.elem.text(this.text);
    }
  }

  isProxy() {
    return false;
  }
}

class ProxyCell {
  constructor(cell) {
    this.cell = cell;
  }

  isProxy() {
    return true;
  }
}

class Table {
  constructor(elem, onCellUpdate, onInsertColumn) {
    this.elem = elem;
    this.onInsertColumn = onInsertColumn;
    this.rows = [];
    this.onCellUpdate = onCellUpdate;
  }

  addRow(columns) {
    var cells = [];
    for (var column in columns) {
      var cell = new Cell(columns[column], this.onCellUpdate);
      if (this.onInsertColumnt !== undefined) {
        onInsertColumn(cell, column);
      }
      cells.push(cell);
    }
    this.rows.push(cells);
    this._drawRow(this.rows.length - 1);
    return this.rows[this.rows.length - 1];
  }

  removeRow(rowId) {
    this.elem.children().eq(rowId).remove();
    var row = this.rows[rowId];
    this.rows.splice(rowId, 1);
    for (var cell of row) {
      if (cell.isProxy()) {
        cell.cell.setRowspan(cell.cell.rowspan - 1)
      }
    }
  }

  removeRowByElem(cell) {
    var index = cell.elem.parent().index();
    return this.removeRow(index);
  }

  insertRow(rowId, columns) {
    var cells = [];
    var ignoreColspan = 0;
    for (var column in columns) {
      if (
        this.rows[rowId] === undefined ||
        !this.rows[rowId][column].isProxy()
      ) {
        var cell = new Cell(columns[column], this.onCellUpdate);
        if (this.onInsertColumnt !== undefined) {
          this.onInsertColumn(cell, column);
        }
        cells.push(cell);
      } else {
        var spanCell = this.at(rowId, column);
        if (ignoreColspan === 0) {
          ignoreColspan = spanCell.colspan;
          spanCell.setRowspan(spanCell.rowspan + 1);
        } else {
          --ignoreColspan;
        }
        cells.push(new ProxyCell(spanCell));
      }
    }
    this.rows.splice(rowId, 0, cells);
    this._drawRow(rowId);
    return this.rows[rowId];
  }

  insertRowAfter(cell, columns) {
    var index = cell.elem.parent().index() + cell.rowspan;
    return this.insertRow(index, columns);
  }

  merge(rowStart, rowEnd, colStart, colEnd) {
    var cell = this.at(rowStart, colStart);
    var newColspan = colEnd - colStart + 1;
    var newRowspan = rowEnd - rowStart + 1;
    if (cell.colspan < newColspan) {
      cell.setColspan(newColspan);
    }
    if (cell.rowspan < newRowspan) {
      cell.setRowspan(rowEnd - rowStart + 1);
    }
    for (let i = rowStart; i <= rowEnd; i++) {
      for (let j = colStart; j <= colEnd; j++) {
        if (i !== rowStart || j !== colStart) {
          this.rows[i][j] = new ProxyCell(cell);
        }
      }
      this._redrawRow(i);
    }
  }

  mergeCells(from, to) {
    var fromRow = from.elem.parent().index();
    var toRow = to.elem.parent().index();
    this.merge(fromRow, toRow, 0, 0); //TODO: support cells
  }

  at(row, col) {
    var cell = this.rows[row][col];
    return cell.isProxy() ? cell.cell : cell;
  }

  _drawRow(row) {
    var rowElem = $("<tr>");
    if (row >= 1) {
      var after = this.elem.children().eq(row - 1);
      rowElem.insertAfter(after);
    } else {
      this.elem.prepend(rowElem);
    }
    for (var cell in this.rows[row]) {
      if (!this.rows[row][cell].isProxy()) {
        cell = this.at(row, cell);
        var cellElem = cell.header ? $("<th>") : $("<td>");
        cell.setElem(cellElem);
        rowElem.append(cellElem);
      }
    }
  }

  _redrawRow(row) {
    this.elem.children().eq(row).remove();
    this._drawRow(row);
  }

  _redraw() {
    this.elem.empty();
    for (var row in this.rows) {
      this._drawRow(row);
    }
  }
}
