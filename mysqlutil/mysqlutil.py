#!/usr/bin/env python2
# coding: utf-8

import os

import MySQLdb
import urllib

from pykit import mysqlconnpool
from pykit import strutil


class ConnectionTypeError(Exception):
    pass


class InvalidLength(Exception):
    pass


def scan_index(connpool, table, result_fields, index_fields, index_values,
               left_open=False, limit=None, index_name=None, use_dict=True, retry=0):

    if type(connpool) == dict:
        # an address
        pool = mysqlconnpool.make(connpool)
    elif isinstance(connpool, mysqlconnpool.MysqlConnectionPool):
        pool = connpool
    else:
        raise ConnectionTypeError

    if len(index_values) != len(index_fields):
        raise InvalidLength('number of index fields and values are not equal')

    req_fields = list(index_fields)
    req_values = list(index_values)

    strict = True
    if limit is None:
        strict = False
        limit = 1024

    while True:
        sql = make_index_scan_sql(table, None, req_fields, req_values,
                             left_open=left_open, limit=limit, index_name=index_name)

        rst = pool.query(sql, retry=retry)

        for rr in rst:
            if use_dict:
                yield dict([(k, rr[k]) for k in result_fields])
            else:
                yield [rr[k] for k in result_fields]

        if strict:
            break

        if len(rst) > 0:
            last_row = rst[-1]
            req_fields = list(index_fields)
            req_values = [last_row[x] for x in req_fields]
            left_open = True
            continue

        req_fields = req_fields[:-1]
        req_values = req_values[:-1]
        if len(req_fields) > 0:
            continue

        break


def make_index_scan_sql(table, result_fields, index, index_values, left_open=False, limit=1024, index_name=None):

    if left_open:
        operator = '>'
    else:
        operator = '>='

    if index_name is not None:
        force_index = index_name
    elif index is not None:
        force_index = 'idx_' + '_'.join(index)
    else:
        force_index = None

    return make_select_sql(table, result_fields, index, index_values, limit, force_index, operator)


def make_mysqldump_in_range(fields, conn, table, path_dump_to, dump_exec, start, end=None):

    conditions = make_sql_condition_in_range(fields, start, end)
    cond_expression = '(' + ') OR ('.join(conditions) + ')'

    if path_dump_to is None:
        rst_path = '{table}.sql'.format(table=urllib.quote_plus(table))
    elif isinstance(path_dump_to, basestring):
        rst_path = path_dump_to
    else:
        rst_path = os.path.join(*path_dump_to)

    if dump_exec is None:
        cmd = 'mysqldump'
    elif isinstance(dump_exec, basestring):
        cmd = dump_exec
    else:
        cmd = os.path.join(*dump_exec)

    return ('{cmd} --host={host} --port={port} --user={user} --password={password} {db} {table} ' +
            '-w {cond} > {rst_path}').format(
        cmd=quote(cmd, "'"),
        host=quote(conn.get('host', ''), "'"),
        port=quote(str(conn.get('port', '')), "'"),
        user=quote(conn.get('user', ''), "'"),
        password=quote(conn.get('passwd', ''), "'"),

        db=quote(conn.get('db', ''), "'"),
        table=quote(table, "'"),
        cond=quote(cond_expression, "'"),
        rst_path=quote(rst_path, "'"),
    )


def make_sql_condition_in_range(fields, start, end=None):

    fld_len = len(fields)

    if len(start) != fld_len:
        raise InvalidLength(
            "the number of fields in 'start' and 'fields' is not equal")

    if end is None:
        end = type(start)([None] * len(start))

    elif len(end) != fld_len:
        raise InvalidLength(
            "the number of fields in 'end' and 'fields' is not equal")

    elif start >= end:
        return []

    fld_ranges = []
    for i in xrange(fld_len):
        fld_ranges.append((fields[i], (start[i], end[i]),))

    conditions = make_range_conditions(fld_ranges)

    result = []
    for cond in conditions:

        sql_conds = []
        for fld, operator, val in cond:
            sql_conds.append(quote(fld, '`') + operator + _safe(val))

        result.append(' AND '.join(sql_conds))

    return result


def make_range_conditions(fld_ranges, left_close=True):

    result = []
    left = 0
    right = 1

    # field range is (start, end), if start equals to end, it is a blank range.
    # continuose blank ranges in the beginning of field ranges should not concat with '>' or '<'.
    n_pref_range_blank_flds = 0
    for fld, _range in fld_ranges:
        if _range[left] == _range[right]:
            n_pref_range_blank_flds += 1
        else:
            break

    first_effective_fld = fld_ranges[n_pref_range_blank_flds][0]

    len_flds = len(fld_ranges)

    # init
    operator = ' > '
    range_side = left
    n_flds_use = len_flds

    while True:

        cond = []
        for fld, _range in fld_ranges[:n_flds_use - 1]:
            cond.append((fld, ' = ', _range[range_side]))

        fld, _range = fld_ranges[n_flds_use - 1]

        if range_side == left and left_close:
            cond.append((fld, ' >= ', _range[left]))
            left_close = False
        else:
            cond.append((fld, operator, _range[range_side]))

        if fld == first_effective_fld:
            # no right boundary
            if _range[right] is None:
                result.append(cond)
                return result

            cond.append((fld, ' < ', _range[right]))

            operator = ' < '
            range_side = right

        if range_side == left:
            n_flds_use -= 1
        else:
            n_flds_use += 1

        result.append(cond)

        if n_flds_use > len_flds:
            break

    return result


def make_sql_condition(fld_vals, operator="=", callback=list):

    cond_expressions = []

    for k, v in fld_vals[:-1]:
        cond_expressions.append(quote(k, "`") + " = " + _safe(v))

    key, value = fld_vals[-1]
    cond_expressions.append(quote(key, "`") + " " + operator + " " + _safe(value))

    return callback(cond_expressions)


def make_insert_sql(table, values, fields=None):

    sql_pattern = "INSERT INTO {tb}{fld_clause} VALUES {val_clause};"

    if isinstance(table, basestring):
        tb = quote(table, '`')
    else:
        db = quote(table[0], '`')
        table_name = quote(table[1], '`')
        tb = db + '.' + table_name

    if fields is not None:
        fld_clause = ' ({flds})'.format(
            flds=', '.join([quote(fld, "`") for fld in fields]))
    else:
        fld_clause = ''

    val_clause = '({vals})'.format(vals=', '.join([_safe(val) for val in values]))

    sql = sql_pattern.format(
        tb=tb, fld_clause=fld_clause, val_clause=val_clause)

    return sql


def make_update_sql(table, values, index, index_values, limit=None):

    sql_pattern = "UPDATE {tb} SET {set_clause}{where_clause}{limit_clause};"

    if isinstance(table, basestring):
        tb = quote(table, '`')
    else:
        db = quote(table[0], '`')
        table_name = quote(table[1], '`')
        tb = db + '.' + table_name

    set_clause = make_sql_condition(values.items(), callback=', '.join)

    if index is not None:
        where_clause = ' WHERE {cond}'.format(
            cond=make_sql_condition(zip(index, index_values), callback=' AND '.join))
    else:
        where_clause = ''

    if limit is not None:
        limit_clause = ' LIMIT {n}'.format(n=limit)
    else:
        limit_clause = ''

    sql = sql_pattern.format(tb=tb, set_clause=set_clause, where_clause=where_clause,
                             limit_clause=limit_clause)

    return sql


def make_delete_sql(table, index, index_values, limit=None):

    sql_pattern = 'DELETE FROM {tb}{where_clause}{limit_clause};'

    if isinstance(table, basestring):
        tb = quote(table, '`')
    else:
        db = quote(table[0], '`')
        table_name = quote(table[1], '`')
        tb = db + '.' + table_name

    if index is not None:
        where_clause = ' WHERE {cond}'.format(
            cond=make_sql_condition(zip(index, index_values), callback=' AND '.join))
    else:
        where_clause = ''

    if limit is not None:
        limit_clause = ' LIMIT {n}'.format(n=limit)
    else:
        limit_clause = ''

    sql = sql_pattern.format(
        tb=tb, where_clause=where_clause, limit_clause=limit_clause)

    return sql


def make_select_sql(table, result_fields, index, index_values,
                        limit=None, force_index=None, operator='='):

    sql_pattern = 'SELECT {rst} FROM {tb}{force_index}{where_clause}{limit_clause};'

    if isinstance(table, basestring):
        tb = quote(table, "`")
    else:
        db = quote(table[0], "`")
        tbl = quote(table[1], "`")
        tb = db + '.' + tbl

    if result_fields is None:
        rst_flds = '*'
    else:
        rst_flds = ', '.join([quote(x, "`") for x in result_fields])

    if force_index is None:
        index_fld = ''
    else:
        index_fld = ' FORCE INDEX (`{idx}`)'.format(idx=force_index)

    if index is not None:
        where_clause = ' WHERE {cond}'.format(
            cond=make_sql_condition(zip(index, index_values), operator, callback=' AND '.join))
    else:
        where_clause = ''

    if limit is not None:
        limit_clause = ' LIMIT {n}'.format(n=limit)
    else:
        limit_clause = ''

    sql = sql_pattern.format(rst=rst_flds, tb=tb, force_index=index_fld, where_clause=where_clause,
                             limit_clause=limit_clause)

    return sql


def quote(s, quote):

    if quote in s:
        s = s.replace(quote, "\\" + quote)

    return quote + s + quote


def _safe(s):
    return '"' + MySQLdb.escape_string(s) + '"'


def make_sharding(conf):

    result = {
            "shard": [],
            "num": [],
            "total": 0,
        }

    db = conf['db']
    table = conf['table']
    shard_fileds = conf['shard_fields']
    start_shard = conf['start_shard']

    # args = [(db, table), result_fields, index_fields, start_index_values]
    args = [(db, table), shard_fileds, shard_fileds, start_shard]
    kwargs = {"left_open": False, "use_dict": False, "retry": 3}

    conn = conf['conn']
    connpool = mysqlconnpool.make(conn)
    records = scan_index(connpool, *args, **kwargs)

    number_per_shard = conf['number_per_shard']
    tolerance = conf['tolerance_of_shard']

    shardings = strutil.sharding(
        records, number_per_shard, accuracy=tolerance, joiner=list)

    sharding_generator = conf.get('sharding_generator', tuple)
    for shard, count in shardings:

        if shard is not None:
            result['shard'].append(sharding_generator(shard))
        else:
            result['shard'].append(sharding_generator(start_shard))

        result['num'].append(count)
        result['total'] += int(count)

    return result
