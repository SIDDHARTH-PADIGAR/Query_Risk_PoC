import re
import sqlparse
import math
from tables_config import tables, table_sizes

RE_WHITESPACE = re.compile(r"\s+")
RE_TABLE = re.compile(r'\bFROM\s+([A-Za-z0-9_\.]+)|\bJOIN\s+([A-Za-z0-9_\.]+)', re.IGNORECASE)
RE_ON_CLAUSE = re.compile(r'\bON\s+([^JRHG]+?)(?=(\bJOIN\b|\bWHERE\b|\bGROUP\b|\bORDER\b|\bHAVING\b|$))', re.IGNORECASE)
RE_OVER = re.compile(r'\bOVER\s*\(', re.IGNORECASE)
RE_WINDOW = re.compile(r'\bRANK\s*\(|\bROW_NUMBER\s*\(|\bNTILE\s*\(', re.IGNORECASE)
RE_UDF = re.compile(r'\bPYTHON\b|\bR\(|\bJAVASCRIPT\b|\bUDF\b', re.IGNORECASE)
RE_S3 = re.compile(r"(s3://|gs://|https?://.+/.*\*)", re.IGNORECASE)
RE_AGG = re.compile(r'\b(SUM|COUNT|AVG|MIN|MAX)\s*\(', re.IGNORECASE)
RE_GROUP_BY = re.compile(r'\bGROUP\s+BY\b', re.IGNORECASE)
RE_ORDER_BY = re.compile(r'\bORDER\s+BY\b', re.IGNORECASE)
RE_LIMIT = re.compile(r'\bLIMIT\b', re.IGNORECASE)

def _normalize(sql: str) -> str:
    s = sql.replace("\n", " ").replace("\t", " ")
    s = RE_WHITESPACE.sub(" ", s).strip()
    return s

def _find_tables(sql_norm: str):
    found = set()
    for m in RE_TABLE.finditer(sql_norm):
        t = m.group(1) or m.group(2)
        if t:
            t = t.split('.')[-1]
            t = t.split()[0].strip().strip(',')
            found.add(t.lower())
    return list(found)

def _count_joins(sql_norm: str):
    return len(re.findall(r'\bJOIN\b', sql_norm, flags=re.IGNORECASE))

def _count_filters(sql_norm: str):
    where_count = 1 if re.search(r'\bWHERE\b', sql_norm, flags=re.IGNORECASE) else 0
    and_count = len(re.findall(r'\bAND\b', sql_norm, flags=re.IGNORECASE))
    or_count = len(re.findall(r'\bOR\b', sql_norm, flags=re.IGNORECASE))
    return where_count + max(0, and_count + or_count - (1 if where_count else 0))

def _subquery_stats(parsed):
    depth = 0
    max_depth = 0
    text = str(parsed)
    for ch in text:
        if ch == '(':
            depth += 1
            if depth > max_depth: max_depth = depth
        elif ch == ')':
            depth = max(0, depth-1)
    selects = len(re.findall(r'\bSELECT\b', text, flags=re.IGNORECASE))
    return max(0, selects-1), max_depth

def _contains_cartesian_on(sql_norm: str):
    for m in RE_ON_CLAUSE.finditer(sql_norm):
        on = m.group(1) or ""
        if '1=1' in on or '=' not in on:
            return 1
    return 0

def _estimate_join_output(table_list, num_joins, num_filters):
    if not table_list:
        return 0
    sizes = [table_sizes.get(t, 1000) for t in table_list]
    max_table = max(sizes)
    filter_selectivity = 0.1 if num_filters>0 else 1.0
    per_join_selectivity = 0.05 if num_joins>0 else 1.0
    est = max_table * filter_selectivity * (per_join_selectivity ** max(0, num_joins-1))
    return max(1, int(est))

def _estimate_sort_cost(rows):
    if rows <= 1:
        return 0.0
    return rows * math.log2(rows)

def extract_metadata(sql: str) -> dict:
    sql_raw = _normalize(sql)
    parsed = sqlparse.parse(sql_raw)
    parsed_stmt = parsed[0] if parsed else None

    used_tables = _find_tables(sql_raw)
    num_tables = len(used_tables)
    num_joins = _count_joins(sql_raw)
    num_filters = _count_filters(sql_raw)
    num_subqueries, subquery_depth = _subquery_stats(parsed_stmt) if parsed_stmt is not None else (0,0)
    num_aggregates = len(RE_AGG.findall(sql_raw))
    has_groupby = 1 if RE_GROUP_BY.search(sql_raw) else 0
    has_orderby = 1 if RE_ORDER_BY.search(sql_raw) else 0
    has_limit = 1 if RE_LIMIT.search(sql_raw) else 0
    select_star = 1 if re.search(r'\bSELECT\s+\*', sql_raw, flags=re.IGNORECASE) else 0
    window_functions = 1 if (RE_OVER.search(sql_raw) or RE_WINDOW.search(sql_raw)) else 0
    udf = 1 if RE_UDF.search(sql_raw) else 0
    s3_scan = 1 if RE_S3.search(sql_raw) else 0
    cartesian = _contains_cartesian_on(sql_raw)

    estimated_table_size_max = max([table_sizes.get(t, 1000) for t in used_tables]) if used_tables else 0
    estimated_join_output = _estimate_join_output(used_tables, num_joins, num_filters)
    estimated_output_rows = estimated_join_output
    if has_groupby and num_aggregates>0:
        estimated_output_rows = max(1, int(estimated_join_output * 0.05))
    estimated_sort_cost = _estimate_sort_cost(estimated_output_rows)

    select_star_columns_estimate = sum(len(tables.get(t, [])) for t in used_tables) if select_star and used_tables else 0

    metadata = {
        "num_tables": num_tables,
        "num_joins": num_joins,
        "num_filters": num_filters,
        "num_subqueries": num_subqueries,
        "subquery_depth": subquery_depth,
        "num_aggregates": num_aggregates,
        "has_groupby": has_groupby,
        "has_orderby": has_orderby,
        "has_limit": has_limit,
        "select_star": select_star,
        "window_functions": window_functions,
        "udf_usage": udf,
        "s3_scan": s3_scan,
        "cartesian_join": cartesian,
        "query_length": len(sql_raw),
        "estimated_table_size_max": estimated_table_size_max,
        "estimated_join_output": estimated_join_output,
        "estimated_output_rows": estimated_output_rows,
        "estimated_sort_cost": estimated_sort_cost,
        "select_star_columns_estimate": select_star_columns_estimate
    }
    return metadata
