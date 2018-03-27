#!/usr/bin/env python2
# coding: utf-8

import os

import MySQLdb
import urllib

from pykit import mysqlconnpool
from pykit import strutil


class ConnectionTypeError(Exception):
    pass


class IndexNotPairs(Exception):
    pass


class InvalidShardLength(Exception):
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
        raise IndexNotPairs

    req_fields = list(index_fields)
    req_values = list(index_values)

    strict = True
    if limit is None:
        strict = False
        limit = 1024

    while True:
        sql = sql_scan_index(table, [], req_fields, req_values,
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


def sql_scan_index(table, result_fields, index_fields, index_values,
                   left_open=False, limit=1024, index_name=None):

    if isinstance(table, basestring):
        table_name = quote(table, "`")
    else:
        db = quote(table[0], "`")
        tbl = quote(table[1], "`")
        table_name = db + '.' + tbl

    rst_flds = ', '.join([quote(x, "`") for x in result_fields])
    if len(rst_flds) == 0:
        rst_flds = '*'

    if index_name is None and index_fields is not None:
        index_name = 'idx_' + '_'.join(index_fields)

    if index_name is None:
        force_index = ''
    else:
        force_index = ' FORCE INDEX (' + quote(index_name, "`") + ')'

    where_conditions = ''
    if index_fields is not None:

        index_pairs = zip(index_fields, index_values)

        if left_open:
            operator = ' > '
        else:
            operator = ' >= '

        prefix = table_name + '.'
        and_conditions = make_condition(
            index_pairs, operator, prefix=prefix)

        where_conditions = ' WHERE ' + and_conditions

    limit = int(limit)

    sql_to_return = ('SELECT ' + rst_flds +
                     ' FROM ' + table_name +
                     force_index +
                     where_conditions +
                     ' LIMIT ' + str(limit))

    return sql_to_return


def make_dump_command_between_shards(shard_fields, conn, table, path_dump_to, dump_exec, start, end=None):

    conditions = make_sql_condition_between_shards(shard_fields, start, end)
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


def make_sql_condition_between_shards(shard_fields, start, end=None):

    shard_len = len(shard_fields)
    common_prefix = None

    if len(start) != shard_len:
        raise InvalidShardLength(
            "the number of fields in 'start' and 'shard_fields' is not equal")

    if end is None:
        end = type(start)([None] * len(start))
        common_prefix = []

    elif len(end) != shard_len:
        raise InvalidShardLength(
            "the number of fields in 'end' and 'shard_fields' is not equal")

    elif start >= end:
        return []

    flds_range = {}
    for i in xrange(shard_len):
        flds_range[shard_fields[i]] = (start[i], end[i])

    if common_prefix is None:
        common_prefix = strutil.common_prefix(start, end, recursive=False)

    # fielld range is (start, end), when start equals to end, it is a blank range
    first_effective_fld = shard_fields[len(common_prefix)]

    conditions = make_range_conditions(flds_range, shard_fields, first_effective_fld)

    result = []
    for cond in conditions:

        sql_conds = []
        for fld, operator, val in cond:
            sql_conds.append(quote(fld, '`') + operator + _safe(val))

        result.append(' AND '.join(sql_conds))

    return result


def make_range_conditions(flds_range, flds, first_effective_fld, left_close=True):

    result = []
    left = 0
    right = 1

    operator = ' > '
    range_side = left
    req_flds = len(flds)

    while True:

        cond = []
        for i in xrange(req_flds - 1):
            fld = flds[i]
            _range = flds_range[fld]
            cond.append((fld, ' = ', _range[range_side]))

        fld = flds[req_flds - 1]
        _range = flds_range[fld]

        if range_side == left and left_close:
            cond.append((fld, ' >= ', _range[left]))
            left_close = False
        else:
            cond.append((fld, operator, _range[range_side]))

        if fld == first_effective_fld:
            # no right boundary
            if _range[right] is None:
                result.append(cond)
                break

            operator = ' < '
            range_side = right

            cond.append((fld, operator, _range[range_side]))

        if range_side == left:
            req_flds -= 1
        else:
            req_flds += 1

        result.append(cond)

        if req_flds > len(flds):
            break

    return result


def make_condition(parameters, operator, prefix=''):

    cond_expressions = []

    for k, v in parameters[:-1]:
        cond_expressions.append(prefix + quote(k, "`") + " = " + _safe(v))

    key, value = parameters[-1]
    cond_expressions.append(prefix + quote(key, "`") + operator + _safe(value))

    return " AND ".join(cond_expressions)


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
