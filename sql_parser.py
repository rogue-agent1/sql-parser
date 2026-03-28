#!/usr/bin/env python3
"""Minimal SQL parser and executor for in-memory tables."""
import sys,re
class DB:
    def __init__(self): self.tables={}
    def create(self,name,columns): self.tables[name]={"columns":columns,"rows":[]}
    def insert(self,name,values): self.tables[name]["rows"].append(values)
    def select(self,name,columns=None,where=None,order_by=None,limit=None):
        t=self.tables[name];cols=t["columns"]
        rows=t["rows"]
        if where:
            col,op,val=where
            ci=cols.index(col)
            if op=='=': rows=[r for r in rows if str(r[ci])==val]
            elif op=='>': rows=[r for r in rows if r[ci]>float(val)]
            elif op=='<': rows=[r for r in rows if r[ci]<float(val)]
        if order_by:
            ci=cols.index(order_by.lstrip('-'))
            rows=sorted(rows,key=lambda r:r[ci],reverse=order_by.startswith('-'))
        if limit: rows=rows[:limit]
        if columns and columns!=['*']:
            idxs=[cols.index(c) for c in columns]
            return columns,[[r[i] for i in idxs] for r in rows]
        return cols,rows
def parse_execute(db,sql):
    sql=sql.strip().rstrip(';')
    if sql.upper().startswith('CREATE'):
        m=re.match(r'CREATE TABLE (\w+)\s*\((.+)\)',sql,re.I)
        if m: db.create(m[1],[c.strip().split()[0] for c in m[2].split(',')]);return "OK"
    elif sql.upper().startswith('INSERT'):
        m=re.match(r'INSERT INTO (\w+) VALUES\s*\((.+)\)',sql,re.I)
        if m:
            vals=[]
            for v in m[2].split(','):
                v=v.strip().strip("'\"")
                try: vals.append(int(v))
                except:
                    try: vals.append(float(v))
                    except: vals.append(v)
            db.insert(m[1],vals);return "OK"
    elif sql.upper().startswith('SELECT'):
        m=re.match(r'SELECT (.+?) FROM (\w+)(?:\s+WHERE\s+(\w+)\s*(=|>|<)\s*(.+?))?(?:\s+ORDER BY\s+(\w+)(?:\s+(DESC))?)?(?:\s+LIMIT\s+(\d+))?\s*$',sql,re.I)
        if m:
            cols=[c.strip() for c in m[1].split(',')]
            where=(m[3],m[4],m[5].strip("'\"")) if m[3] else None
            order=('-'+m[6] if m[7] else m[6]) if m[6] else None
            limit=int(m[8]) if m[8] else None
            return db.select(m[2],cols,where,order,limit)
    return "ERROR"
def main():
    db=DB()
    sqls=["CREATE TABLE users (id, name, age)","INSERT INTO users VALUES (1, 'Alice', 30)",
          "INSERT INTO users VALUES (2, 'Bob', 25)","INSERT INTO users VALUES (3, 'Carol', 35)",
          "SELECT * FROM users","SELECT name, age FROM users WHERE age > 28",
          "SELECT * FROM users ORDER BY age DESC LIMIT 2"]
    for sql in sqls:
        result=parse_execute(db,sql)
        if isinstance(result,tuple):
            cols,rows=result
            print(f"\n{sql}")
            print(f"  {cols}")
            for r in rows: print(f"  {r}")
        else: print(f"{sql} → {result}")
if __name__=="__main__": main()
