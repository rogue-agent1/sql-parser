#!/usr/bin/env python3
"""sql_parser - SQL query engine for CSV files."""
import argparse, csv, re, json, sys, operator

OPS = {'=': operator.eq, '!=': operator.ne, '>': operator.gt, '<': operator.lt,
       '>=': operator.ge, '<=': operator.le}

def parse_query(sql):
    sql = sql.strip().rstrip(';')
    m = re.match(r'SELECT\s+(.+?)\s+FROM\s+(\S+)(?:\s+WHERE\s+(.+?))?(?:\s+ORDER\s+BY\s+(\S+)(?:\s+(ASC|DESC))?)?(?:\s+LIMIT\s+(\d+))?$', sql, re.I)
    if not m: raise ValueError(f"Cannot parse: {sql}")
    cols, table, where, order, direction, limit = m.groups()
    return {
        "columns": [c.strip() for c in cols.split(',')],
        "table": table, "where": where, "order_by": order,
        "descending": direction and direction.upper() == 'DESC',
        "limit": int(limit) if limit else None
    }

def eval_where(row, where):
    if not where: return True
    m = re.match(r"(\w+)\s*(=|!=|>|<|>=|<=)\s*['\"]?([^'\"]+)['\"]?", where)
    if not m: return True
    col, op, val = m.groups()
    rv = row.get(col, '')
    try: rv, val = float(rv), float(val)
    except: pass
    return OPS[op](rv, val)

def execute(query, rows):
    filtered = [r for r in rows if eval_where(r, query['where'])]
    if query['order_by']:
        key = query['order_by']
        def sort_key(r):
            v = r.get(key, '')
            try: return float(v)
            except: return v
        filtered.sort(key=sort_key, reverse=query.get('descending', False))
    if query['limit']: filtered = filtered[:query['limit']]
    cols = query['columns']
    if cols == ['*']: cols = list(rows[0].keys()) if rows else []
    return [{c: r.get(c, '') for c in cols} for r in filtered]

def main():
    p = argparse.ArgumentParser(description="SQL for CSV")
    p.add_argument("sql", help="SQL query (FROM = csv filename)")
    p.add_argument("--json", action="store_true")
    args = p.parse_args()
    query = parse_query(args.sql)
    with open(query['table']) as f: rows = list(csv.DictReader(f))
    results = execute(query, rows)
    if args.json:
        print(json.dumps(results, indent=2))
    else:
        if results:
            cols = list(results[0].keys())
            print('\t'.join(cols))
            for r in results: print('\t'.join(str(r[c]) for c in cols))
        print(f"\n({len(results)} rows)")

if __name__ == "__main__":
    main()
