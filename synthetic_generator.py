import random
import csv
import argparse
import re
from metadata_extractor import extract_metadata
from tables_config import tables, table_sizes

AGG_FUNCS = ["SUM", "COUNT", "AVG", "MIN", "MAX"]
JOIN_TYPES = ["INNER JOIN", "LEFT JOIN", "RIGHT JOIN"]
DATE_SUFFIXES = ("date", "ts", "time", "at")

def pick_table(bias='mixed'):
    keys = list(table_sizes.keys())
    if bias == 'fact':
        weights = [table_sizes[k] for k in keys]
    elif bias == 'dim':
        weights = [1.0/(1+table_sizes[k]) for k in keys]
    else:
        weights = [max(1, min(1000, table_sizes[k] // 1000)) for k in keys]
    total = sum(weights)
    r = random.random() * total
    s = 0
    for k,w in zip(keys,weights):
        s += w
        if r <= s:
            return k
    return random.choice(keys)

def build_pred(col, alias, rows):
    if any(s in col for s in DATE_SUFFIXES):
        if random.random() < 0.6:
            return f"{alias}.{col} > '2021-01-01'"
        return f"{alias}.{col} BETWEEN '2020-01-01' AND '2022-12-31'"
    if "id" in col:
        if random.random() < 0.7:
            return f"{alias}.{col} = {random.randint(1,1000)}"
        vals = ",".join(str(random.randint(1,100)) for _ in range(random.randint(2,4)))
        return f"{alias}.{col} IN ({vals})"
    if any(x in col for x in ("amount","price","qty","stock")):
        return f"{alias}.{col} > {random.randint(1,500)}"
    return f"{alias}.{col} IS NOT NULL"

def make_select_list(alias_cols):
    chosen = random.sample(alias_cols, k=min(len(alias_cols), random.randint(1,4)))
    cols = [f"{a}.{c}" for a,c in chosen]
    if random.random() < 0.5:
        a,c = random.choice(alias_cols)
        cols.append(f"{random.choice(AGG_FUNCS)}({a}.{c})")
    return ", ".join(cols)

def generate_query(shape='low'):
    if shape == 'low':
        num_tables = random.choices([1,2], weights=[0.8,0.2])[0]
        bias = 'dim'
    elif shape == 'medium':
        num_tables = random.randint(1,3)
        bias = 'mixed'
    else:
        num_tables = random.randint(2,5)
        bias = 'fact'

    chosen = [pick_table(bias=bias) for _ in range(num_tables)]
    aliases = [f"t{i}" for i in range(len(chosen))]
    alias_cols = []
    for a,t in zip(aliases, chosen):
        cols = tables.get(t, ["id"])
        for c in cols:
            alias_cols.append((a,c))

    select = make_select_list(alias_cols)
    sql = f"SELECT {select} FROM {chosen[0]} {aliases[0]} "
    for i in range(1, len(chosen)):
        jt = random.choice(JOIN_TYPES)
        left_alias = aliases[0]
        right_alias = aliases[i]
        left_cols = tables.get(chosen[0], [])
        right_cols = tables.get(chosen[i], [])
        on_col = None
        for col in ("user_id","order_id","id","product_id"):
            if col in left_cols and col in right_cols:
                on_col = col
                break
        if on_col:
            on_clause = f"{left_alias}.{on_col} = {right_alias}.{on_col}"
        else:
            leftpk = left_cols[0]
            rightpk = right_cols[0]
            on_clause = f"{left_alias}.{leftpk} = {right_alias}.{rightpk}"
        sql += f"{jt} {chosen[i]} {right_alias} ON {on_clause} "

    preds = []
    for a,t in zip(aliases, chosen):
        cols = tables.get(t, [])
        if cols and random.random() < 0.7:
            for _ in range(random.randint(0,2)):
                c = random.choice(cols)
                preds.append(build_pred(c, a, table_sizes.get(t, 1000)))
    if preds:
        sql += "WHERE " + " AND ".join(preds) + " "

    has_agg = bool(re.search(r'\b(SUM|COUNT|AVG|MIN|MAX)\(', sql, re.IGNORECASE))
    if has_agg and random.random() < 0.8:
        gb = random.choice(alias_cols)
        sql += f"GROUP BY {gb[0]}.{gb[1]} "
        if random.random() < 0.3:
            sql += "HAVING COUNT(1) > 10 "

    if random.random() < 0.4:
        ord_col = random.choice(alias_cols)
        sql += f"ORDER BY {ord_col[0]}.{ord_col[1]} DESC "
    if random.random() < 0.6:
        sql += f"LIMIT {random.choice([10,50,100,1000])} "

    if shape == 'high' and random.random() < 0.35:
        sub = sql
        sql = f"SELECT * FROM ({sub}) sub WHERE sub.{alias_cols[0][1]} IS NOT NULL"

    meta = extract_metadata(sql)
    return sql, meta

def generate_dataset(n=5000, out='synthetic_v3.csv', skew=(0.7,0.2,0.1), seed=42):
    random.seed(seed)
    counts = [int(n*skew[0]), int(n*skew[1]), n - int(n*skew[0]) - int(n*skew[1])]
    rows = []
    for shape, cnt in [('low', counts[0]), ('medium', counts[1]), ('high', counts[2])]:
        for _ in range(cnt):
            sql, meta = generate_query(shape=shape)
            # label rules aligned with advanced features
            score = 0

            # Table size
            if meta['estimated_table_size_max'] >= 20_000_000:
                score += 3
            elif meta['estimated_table_size_max'] >= 5_000_000:
                score += 2
            elif meta['estimated_table_size_max'] >= 500_000:
                score += 1

            # Join complexity
            if meta['num_joins'] >= 3:
                score += 3
            elif meta['num_joins'] == 2:
                score += 2
            elif meta['num_joins'] == 1:
                score += 1

            # Subquery complexity
            if meta['num_subqueries'] >= 2 or meta["subquery_depth"] >= 3:
                score += 3
            elif meta['num_subqueries'] == 1:
                score += 1

            # Aggregations + sort pressure
            if meta['num_aggregates'] > 0 and meta['has_groupby']:
                score += 3
            elif meta['num_aggregates'] > 0:
                score += 1

            # Select star penalty
            if meta['select_star']:
                score += 2

            # Window functions = high memory, always heavy
            if meta['window_functions']:
                score += 3

            # Limit reduces cost slightly
            if meta['has_limit']:
                score -= 1

            # UDF usage = always high risk
            if meta['udf_usage']:
                score += 3

            # S3 wildcard scan = high risk
            if meta['s3_scan']:
                score += 3

            # Cartesian join = catastrophic
            if meta['cartesian_join']:
                score += 5

            # Output rows + sort cost
            if meta['estimated_output_rows'] > 1_000_000:
                score += 2
            if meta['estimated_sort_cost'] > 5_000_000:
                score += 2


            if score <= 2:
                label = 0
            elif score <= 6:
                label = 1
            else:
                label = 2

            row = {'sql': sql}
            row.update(meta)
            row['label'] = label
            rows.append(row)
    random.shuffle(rows)
    keys = list(rows[0].keys())
    with open(out, 'w', newline='', encoding='utf8') as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    return out

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--n', type=int, default=5000)
    parser.add_argument('--out', type=str, default='synthetic_v3.csv')
    parser.add_argument('--seed', type=int, default=42)
    args = parser.parse_args()
    print("writing", generate_dataset(n=args.n, out=args.out, seed=args.seed))
