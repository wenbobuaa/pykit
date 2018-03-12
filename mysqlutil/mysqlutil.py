#!/usr/bin/env python2
# coding: utf-8

import os

import MySQLdb

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

    if type(table) is str:
        table_name = quote(table)
    else:
        table_name = '.'.join([quote(t) for t in table])

    fields_to_return = ', '.join([quote(x) for x in result_fields])
    if len(fields_to_return) == 0:
        fields_to_return = '*'

    force_index = ''
    if index_name is not None:
        force_index = ' FORCE INDEX (' + quote(index_name) + ')'
    elif len(index_fields) > 0:
        force_index = ' FORCE INDEX (' + quote('idx_' + '_'.join(index_fields)) + ')'

    where_conditions = ''
    index_pairs = zip(index_fields, index_values)

    if len(index_pairs) > 0:

        if left_open:
            operator = ' > '
        else:
            operator = ' >= '

        prefix = table_name + '.'
        and_conditions = concat_condition(
            index_pairs, operator, prefix=prefix)

        where_conditions = ' WHERE ' + and_conditions

    limit = int(limit)

    sql_to_return = ('SELECT ' + fields_to_return +
                     ' FROM ' + table_name +
                     force_index +
                     where_conditions +
                     ' LIMIT ' + str(limit))

    return sql_to_return


def sql_dump_between_shards(shard_fields, conn, table, path_dump_to, dump_exec, start, end=None):

    condition_between_shards = sql_condition_between_shards(
        shard_fields, start, end)
    condition = '(' + ') OR ('.join(condition_between_shards) + ')'

    if len(path_dump_to) == 0:
        rst_path = '{table}.sql'.format(table=table)
    else:
        rst_path = os.path.join(*path_dump_to)

    if len(dump_exec) == 0:
        cmd = 'mysqldump'
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


def sql_condition_between_shards(shard_fields, start, end=None):

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

    same_fields = strutil.common_prefix(start, end, recursive=False)
    prefix_condition = ''
    prefix_len = len(same_fields)
    if prefix_len > 0:
        prefix_shards = zip(shard_fields[:prefix_len], start[:prefix_len])
        prefix_condition = concat_condition(prefix_shards, ' = ')
        prefix_condition += " AND "

        shard_fields = shard_fields[prefix_len:]
        start = start[prefix_len:]
        end = end[prefix_len:]

    start_shards = zip(shard_fields, start)
    first_start_condition = concat_condition(
        start_shards, ' >= ')  # left closed
    start_conditions = generate_shards_condition(start_shards[:-1], ' > ')
    start_conditions.insert(0, first_start_condition)

    condition = [prefix_condition + x for x in start_conditions[:-1]]

    if len(end) == 0:
        condition.append(prefix_condition + start_conditions[-1])
        return condition

    end_shards = zip(shard_fields, end)
    end_conditions = generate_shards_condition(end_shards, ' < ')

    condition.append(prefix_condition +
                     start_conditions[-1] + " AND " + end_conditions[-1])

    condition += reversed([prefix_condition + x for x in end_conditions[:-1]])

    return condition


def generate_shards_condition(shards, operator):

    conditions = []
    shards_to_connect = shards[:]
    while len(shards_to_connect) > 0:

        and_condition = concat_condition(shards_to_connect, operator)
        conditions.append(and_condition)

        shards_to_connect = shards_to_connect[:-1]

    return conditions


def concat_condition(shards, operator, prefix=''):

    condition = []

    for s in shards[:-1]:
        condition.append(prefix + quote(s[0]) + " = " + _safe(s[1]))

    s = shards[-1]
    condition.append(prefix + quote(s[0]) + operator + _safe(s[1]))

    return " AND ".join(condition)


def quote(s, quote="`"):

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
    first_shard = conf['first_shard']

    # args = [(db, table), result_fields, index_fields, start_index_values]
    args = [(db, table), shard_fileds, shard_fileds, first_shard]
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
            result['shard'].append(sharding_generator(first_shard))

        result['num'].append(count)
        result['total'] += int(count)

    return result
