#!/usr/bin/env python3
"""SQL parser and in-memory query engine for CSV files."""
import sys, re, csv, os

class Table:
    def __init__(self, columns, rows):
        self.columns = columns; self.rows = rows
    def select(self, cols):
        if cols == ["*"]: return self
        idxs = [self.columns.index(c) for c in cols if c in self.columns]
        new_cols = [self.columns[i] for i in idxs]
        new_rows = [[r[i] for i in idxs] for r in self.rows]
        return Table(new_cols, new_rows)
    def where(self, cond_fn):
        return Table(self.columns, [r for r in self.rows if cond_fn(dict(zip(self.columns, r)))])
    def order_by(self, col, desc=False):
        idx = self.columns.index(col)
        def key(r):
            try: return float(r[idx])
            except: return r[idx]
        return Table(self.columns, sorted(self.rows, key=key, reverse=desc))
    def limit(self, n):
        return Table(self.columns, self.rows[:n])
    def group_by(self, col, aggs):
        idx = self.columns.index(col); groups = {}
        for r in self.rows:
            k = r[idx]; groups.setdefault(k, []).append(r)
        new_cols = [col] + [a[1] for a in aggs]
        new_rows = []
        for k, rows in groups.items():
            row = [k]
            for func, name, target in aggs:
                tidx = self.columns.index(target) if target != "*" else 0
                vals = [float(r[tidx]) for r in rows if r[tidx] not in ("", None)]
                if func == "count": row.append(str(len(rows)))
                elif func == "sum": row.append(str(sum(vals)))
                elif func == "avg": row.append(f"{sum(vals)/len(vals):.2f}" if vals else "0")
                elif func == "min": row.append(str(min(vals)) if vals else "")
                elif func == "max": row.append(str(max(vals)) if vals else "")
            new_rows.append(row)
        return Table(new_cols, new_rows)
    def __str__(self):
        widths = [max(len(str(c)), max((len(str(r[i])) for r in self.rows), default=0)) for i, c in enumerate(self.columns)]
        header = " | ".join(c.ljust(w) for c, w in zip(self.columns, widths))
        sep = "-+-".join("-" * w for w in widths)
        rows = ["  ".join(str(r[i]).ljust(w) for i, w in enumerate(widths)) for r in self.rows]
        return f"{header}\n{sep}\n" + "\n".join(rows)

def load_csv(path):
    with open(path) as f:
        reader = csv.reader(f); headers = next(reader)
        return Table([h.strip() for h in headers], [list(r) for r in reader])

def eval_cond(cond_str):
    m = re.match(r'(\w+)\s*(=|!=|<|>|<=|>=|LIKE)\s*['"]?([^'"]*)['"]?', cond_str.strip(), re.I)
    if not m: return lambda row: True
    col, oper, val = m.group(1), m.group(2).upper(), m.group(3)
    def fn(row):
        rv = row.get(col, "")
        try: rv, val2 = float(rv), float(val)
        except: rv, val2 = str(rv), str(val)
        if oper == "=": return rv == val2
        if oper == "!=": return rv != val2
        if oper == "<": return rv < val2
        if oper == ">": return rv > val2
        if oper == "<=": return rv <= val2
        if oper == ">=": return rv >= val2
        if oper == "LIKE": return val.replace("%", "") in str(rv)
        return True
    return fn

def execute(sql, tables):
    sql = sql.strip().rstrip(";")
    m = re.match(r'SELECT\s+(.+?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+?))?(?:\s+ORDER\s+BY\s+(\w+)(?:\s+(DESC|ASC))?)?(?:\s+LIMIT\s+(\d+))?$', sql, re.I)
    if not m: return f"Cannot parse: {sql}"
    cols_str, table_name, where_str, order_col, order_dir, limit_n = m.groups()
    if table_name not in tables: return f"Table not found: {table_name}"
    t = tables[table_name]
    cols = [c.strip() for c in cols_str.split(",")]
    if where_str: t = t.where(eval_cond(where_str))
    t = t.select(cols)
    if order_col: t = t.order_by(order_col, order_dir and order_dir.upper() == "DESC")
    if limit_n: t = t.limit(int(limit_n))
    return str(t)

def main():
    tables = {}
    for f in sys.argv[1:]:
        if f.endswith(".csv"):
            name = os.path.splitext(os.path.basename(f))[0]
            tables[name] = load_csv(f)
    if not tables and len(sys.argv) > 1:
        print(execute(sys.argv[1], tables)); return
    print(f"Loaded tables: {', '.join(tables.keys())}")
    print("SQL> ", end="", flush=True)
    for line in sys.stdin:
        line = line.strip()
        if line.upper() == "QUIT": break
        if line: print(execute(line, tables))
        print("SQL> ", end="", flush=True)

if __name__ == "__main__": main()
