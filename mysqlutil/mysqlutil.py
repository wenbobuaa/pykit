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

    fields_to_return = ', '.join([quote(x, "`") for x in result_fields])
    if len(fields_to_return) == 0:
        fields_to_return = '*'

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
        and_conditions = buildup_condition(
            index_pairs, operator, prefix=prefix)

        where_conditions = ' WHERE ' + and_conditions

    limit = int(limit)

    sql_to_return = ('SELECT ' + fields_to_return +
                     ' FROM ' + table_name +
                     force_index +
                     where_conditions +
                     ' LIMIT ' + str(limit))

    return sql_to_return


def get_sql_dump_command_between_shards(shard_fields, conn, table, path_dump_to, dump_exec, start, end=None):

    condition_between_shards = get_sql_condition_between_shards(
        shard_fields, start, end)
    condition = '(' + ') OR ('.join(condition_between_shards) + ')'

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
        cond=quote(condition, "'"),
        rst_path=quote(rst_path, "'"),
    )


def get_sql_condition_between_shards(shard_fields, start, end=None):

    if end is not None:
        if len(shard_fields) != len(end):
            raise InvalidShardLength(
                "the number of fields in 'end' and 'shard_fields' is not equal")
        if start >= end:
            return []
    else:
        end = type(start)()

    if len(shard_fields) != len(start):
        raise InvalidShardLength(
            "the number of fields in 'start' and 'shard_fields' is not equal")

    common_prefix = strutil.common_prefix(start, end, recursive=False)
    prefix_condition = ''
    prefix_len = len(common_prefix)
    if prefix_len > 0:
        prefix_param_pairs = zip(shard_fields[:prefix_len], start[:prefix_len])
        prefix_condition = buildup_condition(prefix_param_pairs, ' = ')
        prefix_condition += " AND "

        shard_fields = shard_fields[prefix_len:]
        start = start[prefix_len:]
        end = end[prefix_len:]

    start_param_pairs = zip(shard_fields, start)

    # left closed
    first_start_condition = buildup_condition(start_param_pairs, ' >= ')

    start_conditions = generate_condition_expressions(
        start_param_pairs[:-1], ' > ')
    start_conditions.insert(0, first_start_condition)

    conditions = [prefix_condition + x for x in start_conditions[:-1]]

    if len(end) == 0:
        conditions.append(prefix_condition + start_conditions[-1])
        return conditions

    end_param_pairs = zip(shard_fields, end)
    end_conditions = generate_condition_expressions(end_param_pairs, ' < ')

    conditions.append(prefix_condition +
                     start_conditions[-1] + " AND " + end_conditions[-1])

    conditions += reversed([prefix_condition + x for x in end_conditions[:-1]])

    return conditions


def generate_condition_expressions(parameters, operator):

    conditions = []
    parameters_to_build = parameters[:]
    while len(parameters_to_build) > 0:

        conditions.append(buildup_condition(parameters_to_build, operator))

        parameters_to_build = parameters_to_build[:-1]

    return conditions


def buildup_condition(parameters, operator, prefix=''):

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


def get_sharding(conf):

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
